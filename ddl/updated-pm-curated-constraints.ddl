from pyspark.sql import SparkSession

print("Booting up Spark Session for Schema Evolution...")
# 1. Initialize the Spark Engine explicitly
spark = SparkSession.builder \
    .appName("Ohana_Schema_Evolution") \
    .getOrCreate()

# 2. List of columns that need to safely accept missing data
columns_to_relax = [
    "cell_id", "enb_id", "collection_date", "collection_timestamp", 
    "market", "technology", "dl_prb_utilization_pct", 
    "ul_prb_utilization_pct", "active_ue_count", 
    "rrc_connected_avg", "availability_pct"
]

print("Updating Iceberg schema constraints...")

# 3. Execute the ALTER TABLE commands
for column in columns_to_relax:
    try:
        spark.sql(f"ALTER TABLE ohana.pm_curated ALTER COLUMN {column} DROP NOT NULL")
        print(f"✅ Dropped NOT NULL constraint on: {column}")
    except Exception as e:
        print(f"⚠️ Could not update {column}: {e}")

print("✨ Schema evolution complete. Table is ready for streaming!")