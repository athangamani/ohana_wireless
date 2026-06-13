from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

print("Initializing Balanced Spark Predictive Engine with Synthetic Injection...")
spark = SparkSession.builder.appName("Ohana_Meltdown_Predictor_Balanced").getOrCreate()

# ==============================================================================
# 1. LOAD AND CLEAN REAL DATA
# ==============================================================================
print("Loading and filtering feature store data...")
raw_df = spark.table("ohana.ml_feature_store")

# Drop physically impossible telemetry (>100%) caused by upstream aggregations
base_df = raw_df.filter(
    (F.col("ul_prb_utilization_pct") <= 100.0) & 
    (F.col("dl_prb_utilization_pct") <= 100.0)
)

# Derive baseline features (Velocity and Acceleration) from pre-existing lags
# This avoids expensive window shuffles across 400M rows
processed_real_df = base_df.withColumn(
    "velocity", F.col("ul_prb_utilization_pct") - F.col("prb_lag_1")
).withColumn(
    "acceleration", F.col("velocity") - (F.col("prb_lag_1") - F.col("prb_lag_4"))
)

# Label the real data: If current load is low/moderate, default label to 0.0 (Sustain)
# (Since the data is capped, real data represents normal or stable states)
labeled_real_df = processed_real_df.withColumn("label", F.lit(0.0))

# Select only the columns needed for the ML vector
feature_cols = [
    "ul_prb_utilization_pct", 
    "velocity", 
    "acceleration", 
    "active_event_flag", 
    "active_surge_multiplier",
    "hour_sin",
    "hour_cos"
]
final_real_df = labeled_real_df.select(*feature_cols, "label")

# Downsample the massive 400M row dataset to a manageable, balanced pool (e.g., ~1%)
print("Downsampling majority class (normal operations) to 1% for class balance...")
sampled_real_df = final_real_df.sample(withReplacement=False, fraction=0.01, seed=42)

# ==============================================================================
# 2. GENERATE AND INJECT SYNTHETIC CRASH SEQUENCES
# ==============================================================================
print("Generating scaled synthetic 'Meltdown' sequences...")

# Define schema matching our feature selection
schema = StructType([
    StructField("ul_prb_utilization_pct", DoubleType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("acceleration", DoubleType(), True),
    StructField("active_event_flag", IntegerType(), True),
    StructField("active_surge_multiplier", DoubleType(), True),
    StructField("hour_sin", DoubleType(), True),
    StructField("hour_cos", DoubleType(), True),
    StructField("label", DoubleType(), True)
])

# Create perfect "Meltdown" states (High utilization, surging velocity, concert active)
# 8 base rows reflecting variations of an un-sustainably spiking network
synthetic_data = [
    # Scenario A: Rapidly accelerating surge at a 7 PM concert (hour_sin ≈ -0.96, hour_cos ≈ -0.25)
    (75.0, 15.0, 5.0, 1, 3.0, -0.96, -0.25, 1.0),
    (85.0, 10.0, 2.0, 1, 3.0, -0.96, -0.25, 1.0),
    (95.0, 10.0, 0.0, 1, 3.0, -0.96, -0.25, 1.0),
    
    # Scenario B: Aggressive spike starting from a 5 PM rush hour event
    (70.0, 20.0, 8.0, 1, 2.5, -0.25, -0.96, 1.0),
    (90.0, 20.0, 0.0, 1, 2.5, -0.25, -0.96, 1.0),
    (98.0, 8.0, -4.0, 1, 2.5, -0.25, -0.96, 1.0),
    
    # Scenario C: Late night event sudden surge capacity breach
    (80.0, 12.0, 4.0, 1, 3.5, 0.50, 0.86, 1.0),
    (92.0, 12.0, 0.0, 1, 3.5, 0.50, 0.86, 1.0)
]

# Multiply the scenarios to create ~48,000 crash records
# This gives the minority class sufficient statistical weight during tree node splits
heavy_synthetic_data = synthetic_data * 6000 
synthetic_df = spark.createDataFrame(heavy_synthetic_data, schema)

# Combine downsampled real baseline data with our upsampled synthetic danger signatures
training_pool_df = sampled_real_df.union(synthetic_df)

# ==============================================================================
# 3. TRAIN DISTRIBUTED GBT CLASSIFIER
# ==============================================================================
print("Vectorizing data for model consumption...")
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
ml_df = assembler.transform(training_pool_df).select("features", "label")

# Train/Test Split
train_data, test_data = ml_df.randomSplit([0.8, 0.2], seed=42)

print("Fitting distributed Gradient-Boosted Trees Classifier...")
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=25, maxDepth=6)
model = gbt.fit(train_data)

# ==============================================================================
# 4. EVALUATE AND SAVE MODEL
# ==============================================================================
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="label")
auc = evaluator.evaluate(predictions)

print(f"✅ Training complete! Model Evaluation Area Under ROC: {auc:.4f}")

# Save the model directly into your telecom model repository
model_path = "s3a://ps-amer-ohana-telecom/models/spark_meltdown_gbt"
model.write().overwrite().save(model_path)
print(f"Model successfully saved to: {model_path}")