
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, FloatType

# Initialize Spark
spark = SparkSession.builder.appName('iceberg-events-loader').getOrCreate()

print("Reading JSON directly from S3...")
# IMPORTANT: Update this path to point to where your actual JSON file(s) live!
json_path = "s3a://ps-amer-ohana-telecom/reference/events/" 

# Read the JSON (multiline=True helps if your JSON is pretty-printed)
df = spark.read \
    .option("multiline", "true") \
    .json(json_path)

print("Casting columns to exact Iceberg schema...")
df_cast = df.select(
    df.event_id,
    df.venue_id,
    df.event_name,
    df.event_category,
    df.expected_attendance.cast(IntegerType()),
    df.event_start_ts.cast(TimestampType()),
    df.event_end_ts.cast(TimestampType()),
    df.surge_multiplier_estimate.cast(FloatType())
)

print("Writing directly to Iceberg...")
# Automatically creates the table, converts to Parquet, and registers with Hive Metastore
df_cast.writeTo("ohana.events_calendar") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print("✨ Success! Events Calendar data perfectly loaded into Iceberg.")