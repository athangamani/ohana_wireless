
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv, explode, split, to_date, current_timestamp, when, broadcast, window, avg
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType

# 1. Configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "ps-amer-kafka-hub-corebroker0.ps-amer.a465-9q4k.cloudera.site:9093")
KAFKA_TOPIC = "cell_tower_telemetry"

spark = SparkSession.builder.appName("Ohana_Dual_Stream_Architecture").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Schema Definition (Now including all required columns)
# Update the schema definition to match the table exactly
telemetry_schema = StructType() \
    .add("cell_id", StringType()) \
    .add("collection_timestamp", TimestampType()) \
    .add("technology", StringType()) \
    .add("dl_prb_utilization_pct", FloatType()) \
    .add("ul_prb_utilization_pct", FloatType()) \
    .add("dl_throughput_mbps", FloatType()) \
    .add("active_ue_count", IntegerType()) \
    .add("rrc_connected_avg", FloatType()) \
    .add("availability_pct", FloatType())

# 3. Read & Parse from Kafka
raw_kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="athangamani" password="BlancaLake123";') \
    .load()

csv_string_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as csv_payload")
exploded_df = csv_string_df.withColumn("single_row", explode(split(col("csv_payload"), "\n")))
csv_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"}

parsed_df = exploded_df \
    .withColumn("data", from_csv(col("single_row"), telemetry_schema.simpleString(), csv_options)) \
    .select("data.*") \
    .filter((col("cell_id").isNotNull()) & (col("cell_id") != "cell_id"))

# 4. Enrich with Static Topology & Data Quality Flags
topology_df = spark.read.table("ohana.topology").select("cell_id", "enb_id", "market")
enriched_df = parsed_df.join(broadcast(topology_df), "cell_id", "left")

base_curated_df = enriched_df \
    .withColumn("collection_date", to_date(col("collection_timestamp"))) \
    .withColumn("load_ts", current_timestamp()) \
    .withColumn("is_outage", when(col("availability_pct") < 95.0, True).otherwise(False)) \
    .withColumn("is_saturated", when(col("dl_prb_utilization_pct") > 85.0, True).otherwise(False)) \
    .withColumn("dq_passed", when(col("dl_prb_utilization_pct").isNotNull(), True).otherwise(False))

# ========================================================================================
# STREAM A: The Historical Archive (Writes to pm_curated)
# ========================================================================================
print("Starting Stream A: Routing raw historical data to ohana.pm_curated...")
curated_output = base_curated_df.select(
    "cell_id", "enb_id", "collection_date", "collection_timestamp", "market", 
    "technology", "dl_prb_utilization_pct", "ul_prb_utilization_pct", 
    "dl_throughput_mbps", "active_ue_count", "rrc_connected_avg", 
    "availability_pct", "is_outage", "is_saturated", "dq_passed", "load_ts"
)

stream_a = curated_output.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://ps-amer-ohana-telecom/checkpoints/pm_curated_v3/") \
    .toTable("ohana.pm_curated")

# ========================================================================================
# STREAM B: The Live Inference Speed Layer (Writes to live_inference_stream)
# ========================================================================================
print("Starting Stream B: Routing 15-minute rolling aggregations to ohana.live_inference_stream...")
rolling_features_df = base_curated_df \
    .withWatermark("collection_timestamp", "10 minutes") \
    .groupBy(window(col("collection_timestamp"), "15 minutes", "5 minutes"), col("cell_id")) \
    .agg(
        avg("ul_prb_utilization_pct").alias("rolling_ul_utilization_pct"),
        avg("dl_throughput_mbps").alias("rolling_dl_throughput_mbps"),
        avg("active_ue_count").alias("rolling_active_ue_count")
    )

inference_output = rolling_features_df.select(
    col("cell_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("rolling_ul_utilization_pct"),
    col("rolling_dl_throughput_mbps"),
    col("rolling_active_ue_count")
)

stream_b = inference_output.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://ps-amer-ohana-telecom/checkpoints/live_inference_v3/") \
    .toTable("ohana.live_inference_stream")

# Await termination for BOTH streams
spark.streams.awaitAnyTermination()