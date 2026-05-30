
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName('iceberg-topology-loader').getOrCreate()

print("Reading CSV directly from S3...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://ps-amer-ohana-telecom/reference/topology/topology.csv")

print("Writing directly to Iceberg...")
# This automatically handles schema creation, Parquet conversion, and HMS registration!
df.writeTo("ohana.topology") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print("✨ Success! Topology data perfectly loaded into Iceberg.")