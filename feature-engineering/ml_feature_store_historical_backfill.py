
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, broadcast

# 1. Initialize Spark for a Heavy Batch Job
spark = SparkSession.builder \
    .appName('ohana-historical-event-backfill') \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Booting up Historical Backfill Job for 440M Rows...")

# 2. Load Existing Lakehouse Data
features_df = spark.read.table("ohana.ml_feature_store")
topology_df = spark.read.table("ohana.topology")
events_df = spark.read.table("ohana.events_calendar")

# 3. The Temporal-Spatial Join
print("Mapping cell towers to venues...")
# Broadcast the topology because it is very small, speeding up the join over 440M rows
features_with_venues = features_df.join(
    broadcast(topology_df.select("cell_id", "special_venue_id")), 
    "cell_id", 
    "left"
)

print("Executing heavy temporal join against historical events...")
e = events_df.alias("e")

# Does the tower cover the venue? AND is the telemetry timestamp during the event window?
features_events_raw = features_with_venues.join(
    e,
    (features_with_venues.special_venue_id == e.venue_id) &
    (features_with_venues.collection_timestamp >= e.event_start_ts) &
    (features_with_venues.collection_timestamp <= e.event_end_ts),
    "left"
)

# 4. Construct the Final Event Columns
print("Calculating active_event_flag, surge_multipliers, and attendance...")
features_final = features_events_raw.withColumn(
    "active_event_flag",
    when(col("e.event_id").isNotNull(), lit(1)).otherwise(lit(0))
).withColumn(
    "active_surge_multiplier",
    coalesce(col("e.surge_multiplier_estimate"), lit(1.0))
).withColumn(
    "active_attendance",
    coalesce(col("e.expected_attendance"), lit(0))
)

# 5. Clean up Join Artifacts
columns_to_drop = [
    "special_venue_id", "event_id", "venue_id", "event_name", 
    "event_category", "event_start_ts", "event_end_ts", "surge_multiplier_estimate",
    "expected_attendance" 
]
df_to_write = features_final.drop(*columns_to_drop)

# 6. Overwrite the Iceberg Table
print("Overwriting ohana.ml_feature_store with updated event-aware schema...")
# Iceberg's .replace() safely swaps the new schema and data into the existing table name
df_to_write.writeTo("ohana.ml_feature_store") \
    .using("iceberg") \
    .replace()

print("✨ Massive Historical Backfill Complete! Table is now ready for students.")