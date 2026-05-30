from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    FloatType, IntegerType, BooleanType, DateType
)

# 1. Initialize Spark Session (CDE handles the Iceberg catalog configs automatically)
spark = SparkSession.builder \
    .appName("PM_Kafka_to_Iceberg_Stream") \
    .getOrCreate()

# Set log level to WARN to prevent terminal spam
spark.sparkContext.setLogLevel("WARN")

# Define Kafka Variables
KAFKA_BROKERS = "ps-amer-kafka-hub-corebroker0.ps-amer.a465-9q4k.cloudera.site:9093" # Update with your exact broker
KAFKA_TOPIC = "network.pm.raw"
ICEBERG_TABLE = "ohana.pm_curated"
CHECKPOINT_LOCATION = "s3a://ps-amer-ohana-telecom/checkpoints/pm_curated_stream/"

# 2. Define the exact schema from your NiFi/Impala setup
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

# 3. THE SOURCE: Read from Kafka
# We use "earliest" to process the historical data NiFi already fanned out
# 3. THE SOURCE: Read from Kafka
print(f"Subscribing to Kafka Topic: {KAFKA_TOPIC}...")
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

# 4. THE TRANSFORMATION: Crack the JSON and format the DataFrame
# Kafka payload is in binary. Cast to string -> parse JSON -> expand columns
parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .withColumn("data", from_json(col("json_string"), pm_schema)) \
    .select("data.*") \
    .withColumn("load_ts", current_timestamp()) # Add the missing load timestamp

# 5. THE SINK: Write continuously to Iceberg
# 5. THE SINK: Write continuously to Iceberg
print(f"Streaming data into Iceberg Table: {ICEBERG_TABLE}...")
query = parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .option("check-nullability", "false") \
    .toTable(ICEBERG_TABLE)

# Keep the streaming job running
query.awaitTermination()
