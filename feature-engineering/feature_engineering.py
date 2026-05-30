import math
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sin, cos, lit, hour, dayofweek,
    when, coalesce, broadcast, max as spark_max, unix_timestamp
)

# 1. Initialize Spark Session
spark = SparkSession.builder.appName('ohana-ml-feature-engineering').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Loading curated telemetry and reference data...")
pm_df = spark.read.table("ohana.pm_curated")
neighbors_df = spark.read.table("ohana.cell_neighbors")
topology_df = spark.read.table("ohana.topology")
events_df = spark.read.table("ohana.events_calendar")

# ==============================================================================
# CONSTRAINT 2: Actively filter out outages to prevent zero-traffic downward bias
# ==============================================================================
print("Filtering outage anomalies...")
pm_clean = pm_df.filter(col("is_outage") == False)

# 2. Base Time Features & Cyclical Encoding
print("Applying cyclical temporal encoding...")
pm_time = pm_clean.withColumn("hour_of_day", hour(col("collection_timestamp"))) \
                  .withColumn("day_of_week", dayofweek(col("collection_timestamp")))

pi = math.pi
pm_encoded = pm_time.withColumn("hour_sin", sin(lit(2 * pi) * col("hour_of_day") / 24)) \
                    .withColumn("hour_cos", cos(lit(2 * pi) * col("hour_of_day") / 24)) \
                    .withColumn("dow_sin", sin(lit(2 * pi) * col("day_of_week") / 7)) \
                    .withColumn("dow_cos", cos(lit(2 * pi) * col("day_of_week") / 7))

# ==============================================================================
# CONSTRAINT 1: Strict Window partitioning and ordering to prevent data leakage
# ==============================================================================
print("Computing time-series lags and rolling statistics...")
w_lag = Window.partitionBy("cell_id").orderBy("collection_timestamp")

# Calculate Lags
pm_lags = pm_encoded \
    .withColumn("prb_lag_1", lag("dl_prb_utilization_pct", 1).over(w_lag)) \
    .withColumn("prb_lag_4", lag("dl_prb_utilization_pct", 4).over(w_lag)) \
    .withColumn("prb_lag_8", lag("dl_prb_utilization_pct", 8).over(w_lag)) \
    .withColumn("prb_lag_96", lag("dl_prb_utilization_pct", 96).over(w_lag)) \
    .withColumn("prb_lag_672", lag("dl_prb_utilization_pct", 672).over(w_lag))

# Calculate Rolling Stats (Using rowsBetween(-4, -1) to strictly look at the PAST)
w_1hr = Window.partitionBy("cell_id").orderBy("collection_timestamp").rowsBetween(-4, -1)
w_24hr = Window.partitionBy("cell_id").orderBy("collection_timestamp").rowsBetween(-96, -1)

pm_rolling = pm_lags \
    .withColumn("prb_roll_mean_1hr", avg("dl_prb_utilization_pct").over(w_1hr)) \
    .withColumn("prb_roll_std_1hr", stddev("dl_prb_utilization_pct").over(w_1hr)) \
    .withColumn("prb_roll_mean_24hr", avg("dl_prb_utilization_pct").over(w_24hr)) \
    .withColumn("prb_roll_std_24hr", stddev("dl_prb_utilization_pct").over(w_24hr))

# 3. Spatial Correlation (Neighbor PRB)
print("Computing spatial correlation features...")
# Grab the t-1 lag of the neighbors so we don't leak the current interval's traffic
neighbor_prb = pm_rolling.select(
    col("cell_id").alias("neighbor_cell_id"),
    col("collection_timestamp"),
    col("prb_lag_1").alias("neighbor_prb_t1")
)

spatial_df = neighbors_df.join(
    neighbor_prb,
    on="neighbor_cell_id",
    how="inner"
).groupBy("cell_id", "collection_timestamp").agg(
    avg("neighbor_prb_t1").alias("neighbor_avg_prb_util_t1"),
    spark_max("neighbor_prb_t1").alias("neighbor_max_prb_util_t1")
)

pm_spatial = pm_rolling.join(spatial_df, on=["cell_id", "collection_timestamp"], how="left")

# 4. Event Proximity Integration
print("Integrating event proximity features...")
# Join topology to get the special_venue_id for each cell
pm_with_venues = pm_spatial.join(
    broadcast(topology_df.select("cell_id", "special_venue_id")), # <-- REMOVED confidence_tier
    on="cell_id",
    how="left"
)

# Cross-reference with events calendar
# Note: In a true production script, this is often done via a windowed join or UDF to find the *next* event. 
# Assuming `events_df` has been pre-joined/processed to provide the nearest event timestamp per venue:
events_subset = events_df.select(
    col("venue_id").alias("special_venue_id"),
    col("event_start_ts")
)

pm_events = pm_with_venues.join(events_subset, on="special_venue_id", how="left")

# Calculate hours until the next event
pm_events = pm_events.withColumn(
    "raw_hours_to_event",
    (unix_timestamp("event_start_ts") - unix_timestamp("collection_timestamp")) / 3600
)

# Only keep future events (where hours > 0)
pm_events = pm_events.withColumn(
    "valid_hours_to_event",
    when(col("raw_hours_to_event") > 0, col("raw_hours_to_event")).otherwise(None)
)

# ==============================================================================
# CONSTRAINT 3: Use -1.0 as the sentinel value for cells not near an event
# ==============================================================================
pm_final = pm_events.withColumn(
    "hours_to_next_event",
    coalesce(col("valid_hours_to_event"), lit(-1.0))
).withColumn(
    "confidence_tier", 
    lit("HIGH")  # <-- ADDED ML TIER
)

# 5. Write to Iceberg ML Feature Store
print("Writing engineered features to ohana.ml_feature_store...")

# Drop intermediate calculation columns before writing
columns_to_drop = ["hour_of_day", "day_of_week", "special_venue_id", "event_start_ts", "raw_hours_to_event", "valid_hours_to_event"]
df_to_write = pm_final.drop(*columns_to_drop)

df_to_write.writeTo("ohana.ml_feature_store") \
    .using("iceberg") \
    .partitionedBy("market") \
    .createOrReplace() # Use append() if running daily incrementally

print("✨ ML Feature Engineering Pipeline Complete.")
