from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg, sum
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    FloatType, IntegerType, BooleanType, DateType
)

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("PM_5Min_KPI_Stream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Variables
KAFKA_BROKERS = "ps-amer-kafka-hub-corebroker0.ps-amer.a465-9q4k.cloudera.site:9093" 
KAFKA_TOPIC = "network.pm.raw"
ICEBERG_AGG_TABLE = "ohana.streaming_kpi_agg"
CHECKPOINT_LOCATION = "s3a://ps-amer-ohana-telecom/checkpoints/kpi_agg_stream/"

# 2. Schema Definition (Same as before)
pm_schema = StructType([
    StructField("cell_id", StringType(), False),
    StructField("enb_id", StringType(), False),
    StructField("collection_date", DateType(), False),
    StructField("collection_timestamp", TimestampType(), False),
    StructField("market", StringType(), False),
    StructField("technology", StringType(), False),
    StructField("dl_prb_utilization_pct", FloatType(), False),
    StructField("ul_prb_utilization_pct", FloatType(), False),
    StructField("dl_throughput_mbps", FloatType(), True),
    StructField("active_ue_count", IntegerType(), False),
    StructField("rrc_connected_avg", FloatType(), False),
    StructField("availability_pct", FloatType(), False),
    StructField("is_outage", BooleanType(), False),
    StructField("is_saturated", BooleanType(), False),
    StructField("dq_passed", BooleanType(), False)
])

# 3. Read from Kafka (JAAS Auth Included)
print(f"Subscribing to Kafka Topic for Aggregation: {KAFKA_TOPIC}...")
raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="athangamani" password="BlancaLake123";') \
    .load()

# 4. Parse the JSON
parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .withColumn("data", from_json(col("json_string"), pm_schema)) \
    .select("data.*")

from pyspark.sql.functions import max, expr, lit # Make sure to add these to your imports at the top!

# 5. THE TRANSFORMATION: Watermark and Sliding Window
agg_df = parsed_df \
    .withWatermark("collection_timestamp", "10 minutes") \
    .groupBy(
        window(col("collection_timestamp"), "5 minutes"),
        col("cell_id"),
        col("market")
    ) \
    .agg(
        avg("dl_prb_utilization_pct").cast("float").alias("dl_prb_util_mean"), # <-- Cast added
        max("dl_prb_utilization_pct").alias("dl_prb_util_max"),
        expr("percentile_approx(dl_prb_utilization_pct, 0.95)").alias("dl_prb_util_p95"),
        max("active_ue_count").alias("active_ue_count_max"),
        avg("dl_throughput_mbps").cast("float").alias("dl_throughput_mean")    # <-- Cast added
    ) \
    .select(
        col("cell_id"),
        col("market"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("dl_prb_util_mean"),
        col("dl_prb_util_max"),
        col("dl_prb_util_p95"),
        col("active_ue_count_max"),
        col("dl_throughput_mean"),
        lit(False).alias("anomaly_flag")
    )

# 6. Write to Iceberg
print(f"Streaming aggregations into Iceberg Table: {ICEBERG_AGG_TABLE}...")
query = agg_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .option("check-nullability", "false") \
    .toTable(ICEBERG_AGG_TABLE)

query.awaitTermination()
