from pyspark.sql import SparkSession
import os

S3_BUCKET   = 'ps-amer-ohana-telecom'

# 1. Initialize the Spark Session FIRST
spark = SparkSession.builder \
    .appName('ohana-cleanup-job') \
    .getOrCreate()

# 2. NOW we can use 'spark' to access the JVM and wipe the data
print("Connecting to JVM FileSystem...")
URI = spark._jvm.java.net.URI
Path = spark._jvm.org.apache.hadoop.fs.Path
FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
conf = spark.sparkContext._jsc.hadoopConfiguration()

fs = FileSystem.get(URI(f"s3a://{S3_BUCKET}"), conf)
target_path = Path(f"s3a://{S3_BUCKET}/raw/ran-pm/")

if fs.exists(target_path):
    print(f"🧹 Sweeping existing data at {target_path}...")
    fs.delete(target_path, True)  # True = recursive delete
    print("✨ Cleanup complete!")
else:
    print("No existing data found. Proceeding.")
# 
