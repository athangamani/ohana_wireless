import argparse
import os
import math
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sin, cos, lit, hour, dayofweek,
    when, coalesce, broadcast, max as spark_max, unix_timestamp, to_date, date_sub
)

# ==============================================================================
# 1. PARSE THE AIRFLOW EXECUTION DATE
# ==============================================================================
parser = argparse.ArgumentParser()
parser.add_argument('--execution_date', type=str, required=True, help='Date to process (YYYY-MM-DD)')
args = parser.parse_args()
target_date = args.execution_date

print(f"Executing job for target date: {target_date}")

spark = SparkSession.builder.appName(f'ohana-ml-feature-incremental-{target_date}').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# 2. PARTITION PRUNING (Read ONLY the last 8 days of data to save compute)
# ==============================================================================
print("Loading reference data and 8-day lookback window from pm_curated...")
pm_df = spark.read.table("ohana.pm_curated") \
    .filter(col("collection_date") >= date_sub(to_date(lit(target_date)), 8)) \
    .filter(col("collection_date") <= to_date(lit(target_date)))

neighbors_df = spark.read.table("ohana.cell_neighbors")
# Ensure we use the correct topology table name
topology_df = spark.read.table("ohana.topology") 
events_df = spark.read.table("ohana.events_calendar")

# ==============================================================================
# 3. FILTER OUTAGES AND ENCODE TIME WAVES
# ==============================================================================
pm_clean = pm_df.filter(col("is_outage") == False)
pm_time = pm_clean.withColumn("hour_of_day", hour(col("collection_timestamp"))) \
                  .withColumn("day_of_week", dayofweek(col("collection_timestamp")))

pi = math.pi
pm_encoded = pm_time.withColumn("hour_sin", sin(lit(2 * pi) * col("hour_of_day") / 24)) \
                    .withColumn("hour_cos", cos(lit(2 * pi) * col("hour_of_day") / 24)) \
                    .withColumn("dow_sin", sin(lit(2 * pi) * col("day_of_week") / 7)) \
                    .withColumn("dow_cos", cos(lit(2 * pi) * col("day_of_week") / 7))

# ==============================================================================
# 4. STRICT WINDOWING FOR LAGS AND ROLLING STATS
# ==============================================================================
w_lag = Window.partitionBy("cell_id").orderBy("collection_timestamp")
w_1hr = Window.partitionBy("cell_id").orderBy("collection_timestamp").rowsBetween(-4, -1)
w_24hr = Window.partitionBy("cell_id").orderBy("collection_timestamp").rowsBetween(-96, -1)

pm_rolling = pm_encoded \
    .withColumn("prb_lag_1", lag("dl_prb_utilization_pct", 1).over(w_lag)) \
    .withColumn("prb_lag_4", lag("dl_prb_utilization_pct", 4).over(w_lag)) \
    .withColumn("prb_lag_8", lag("dl_prb_utilization_pct", 8).over(w_lag)) \
    .withColumn("prb_lag_96", lag("dl_prb_utilization_pct", 96).over(w_lag)) \
    .withColumn("prb_lag_672", lag("dl_prb_utilization_pct", 672).over(w_lag)) \
    .withColumn("prb_roll_mean_1hr", avg("dl_prb_utilization_pct").over(w_1hr)) \
    .withColumn("prb_roll_std_1hr", stddev("dl_prb_utilization_pct").over(w_1hr)) \
    .withColumn("prb_roll_mean_24hr", avg("dl_prb_utilization_pct").over(w_24hr)) \
    .withColumn("prb_roll_std_24hr", stddev("dl_prb_utilization_pct").over(w_24hr))

# ==============================================================================
# 5. SPATIAL JOINS (Neighbor Averages)
# ==============================================================================
neighbor_prb = pm_rolling.select(
    col("cell_id").alias("neighbor_cell_id"),
    col("collection_timestamp"),
    col("prb_lag_1").alias("neighbor_prb_t1")
)
spatial_df = neighbors_df.join(neighbor_prb, "neighbor_cell_id", "inner") \
    .groupBy("cell_id", "collection_timestamp") \
    .agg(avg("neighbor_prb_t1").alias("neighbor_avg_prb_util_t1"),
         spark_max("neighbor_prb_t1").alias("neighbor_max_prb_util_t1"))

pm_spatial = pm_rolling.join(spatial_df, ["cell_id", "collection_timestamp"], "left")

# ==============================================================================
# 6. EVENT PROXIMITY & TEMPORAL MAPPING (The Fix)
# ==============================================================================
print("Executing temporal-spatial join to identify active events...")

# Join telemetry to topology to get the venue ID for each cell tower
pm_with_venues = pm_spatial.join(broadcast(topology_df.select("cell_id", "special_venue_id")), "cell_id", "left")

# Alias the events dataframe to avoid column ambiguity during the join
e = events_df.alias("e")

# Perform the temporal-spatial join: 
# Does the tower cover the venue? AND is the telemetry timestamp during the event window?
pm_events_raw = pm_with_venues.join(
    e,
    (pm_with_venues.special_venue_id == e.venue_id) &
    (pm_with_venues.collection_timestamp >= e.event_start_ts) &
    (pm_with_venues.collection_timestamp <= e.event_end_ts),
    "left"
)

# Calculate the new event features based on the join results
pm_events = pm_events_raw.withColumn(
    "active_event_flag",
    when(col("e.event_id").isNotNull(), lit(1)).otherwise(lit(0))
).withColumn(
    "active_surge_multiplier",
    coalesce(col("e.surge_multiplier_estimate"), lit(1.0))
).withColumn(
    "active_attendance",
    coalesce(col("e.expected_attendance"), lit(0))
)

# Optional: Keep the original "hours_to_next_event" logic if you still use it elsewhere
# We calculate it against the start of the event if one is found
pm_events = pm_events.withColumn("raw_hours_to_event", (unix_timestamp("e.event_start_ts") - unix_timestamp("collection_timestamp")) / 3600) \
                     .withColumn("valid_hours_to_event", when(col("raw_hours_to_event") > 0, col("raw_hours_to_event")).otherwise(None))

pm_final = pm_events.withColumn("hours_to_next_event", coalesce(col("valid_hours_to_event"), lit(-1.0))) \
                    .withColumn("confidence_tier", lit("HIGH"))

# ==============================================================================
# 7. THE INCREMENTAL FILTER & WRITE
# ==============================================================================
columns_to_drop = [
    "hour_of_day", "day_of_week", "special_venue_id", "raw_hours_to_event", 
    "valid_hours_to_event", "event_id", "venue_id", "event_name", 
    "event_category", "event_start_ts", "event_end_ts", "surge_multiplier_estimate",
    "expected_attendance" # Drop the raw column, we keep 'active_attendance'
]

# We calculated 8 days of lags, but we ONLY want to save the target date's rows to the feature store
df_to_write = pm_final.drop(*columns_to_drop) \
                      .filter(col("collection_date") == to_date(lit(target_date)))

print(f"Appending curated features for {target_date} to ohana.ml_feature_store...")
df_to_write.writeTo("ohana.ml_feature_store") \
    .using("iceberg") \
    .append()

print("✨ Incremental ML Feature Engineering Complete.")