from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName('iceberg-neighbors-loader').getOrCreate()

print("Reading Cell Neighbors CSV directly from S3...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://ps-amer-ohana-telecom/reference/neighbors/cell_neighbors.csv")

print("Writing directly to Iceberg...")
# Automatically handles the schema creation, Parquet formatting, and HMS registration
df.writeTo("ohana.cell_neighbors") \
    .using("iceberg") \
    .tableProperty("format-version", "2") \
    .createOrReplace()

print("✨ Success! Cell Neighbors data perfectly loaded into Iceberg.")
