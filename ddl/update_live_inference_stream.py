from pyspark.sql import SparkSession

print("Booting up Spark Session for Live Inference Schema Evolution...")
# 1. Initialize the Spark Engine
spark = SparkSession.builder \
    .appName("Ohana_Schema_Evolution_Live") \
    .getOrCreate()

# 2. The three columns that Stream B needs to be optional
columns_to_relax = [
    "cell_id", 
    "window_start", 
    "window_end"
]

print("Updating ohana.live_inference_stream constraints...")

# 3. Execute the ALTER TABLE commands
for column in columns_to_relax:
    try:
        spark.sql(f"ALTER TABLE ohana.live_inference_stream ALTER COLUMN {column} DROP NOT NULL")
        print(f"✅ Dropped NOT NULL constraint on: {column}")
    except Exception as e:
        print(f"⚠️ Could not update {column}: {e}")

print("✨ Schema evolution complete. Stream B is ready to flow!")