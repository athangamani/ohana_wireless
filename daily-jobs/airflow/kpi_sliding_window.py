import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max as spark_max, expr, lit, to_date

# ==============================================================================
# 1. PARSE THE AIRFLOW EXECUTION DATE
# ==============================================================================
parser = argparse.ArgumentParser()
parser.add_argument('--execution_date', type=str, required=True, help='Date to process (YYYY-MM-DD)')
args = parser.parse_args()
target_date = args.execution_date

print(f"Executing job for target date: {target_date}")

spark = SparkSession.builder.appName(f"PM_Daily_KPI_Batch_{target_date}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ICEBERG_AGG_TABLE = "ohana.streaming_kpi_agg"

# ==============================================================================
# 2. READ YESTERDAY'S HISTORICAL DATA AND FILTER OUT NULL MARKETS
# ==============================================================================
pm_df = spark.read.table("ohana.pm_curated") \
    .filter(col("collection_date") == to_date(lit(target_date))) \
    .filter(col("market").isNotNull()) # <-- THE FIX: Drop orphaned records

if pm_df.count() == 0:
    print(f"No data found for {target_date}. Exiting gracefully.")
    spark.stop()
    exit(0)

# ==============================================================================
# 3. CALCULATE KPIs (Batch Aggregation)
# ==============================================================================
# We still use the window function, but now it acts on a static dataset
agg_df = pm_df \
    .groupBy(
        window(col("collection_timestamp"), "5 minutes"),
        col("cell_id"),
        col("market")
    ) \
    .agg(
        avg("dl_prb_utilization_pct").cast("float").alias("dl_prb_util_mean"),
        spark_max("dl_prb_utilization_pct").alias("dl_prb_util_max"),
        expr("percentile_approx(dl_prb_utilization_pct, 0.95)").alias("dl_prb_util_p95"),
        spark_max("active_ue_count").alias("active_ue_count_max"),
        avg("dl_throughput_mbps").cast("float").alias("dl_throughput_mean")
    ) \
    .select(
        col("cell_id"),
        col("market"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("dl_prb_util_mean"),
        col("dl_prb_util_max"),
        col("dl_prb_util_p95"),
        col("active_ue_count_max"),
        col("dl_throughput_mean"),
        lit(False).alias("anomaly_flag")
    )

# ==============================================================================
# 4. APPEND TO ICEBERG
# ==============================================================================
print(f"Appending batch aggregations to Iceberg Table: {ICEBERG_AGG_TABLE}...")

# Use standard batch append instead of writeStream
agg_df.writeTo(ICEBERG_AGG_TABLE) \
    .using("iceberg") \
    .append()

print(f"✨ KPI Generation Complete for {target_date}.")
spark.stop()