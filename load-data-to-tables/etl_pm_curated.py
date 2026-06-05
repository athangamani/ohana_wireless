from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, regexp_extract, to_timestamp, expr, to_date, broadcast, lit, current_timestamp, regexp_replace

# 1. Initialize Spark Session
spark = SparkSession.builder.appName('ohana-batch-dq-pipeline').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

S3_RAW_PATH = "s3a://ps-amer-ohana-telecom/raw/ran-pm/*/*.xml.gz"
S3_TOPO_PATH = "s3a://ps-amer-ohana-telecom/reference/topology/topology.csv"

print("Loading Reference Topology for enrichment...")
df_topo = spark.read.csv(S3_TOPO_PATH, header=True)
df_topo_enrich = df_topo.select(
    col("cell_id"),
    col("market"),
    col("carrier_technology").alias("technology"),
    col("carrier_band").alias("frequency_band") 
)

print(f"Spinning up workers to ingest 175M historical records from {S3_RAW_PATH}...")

# 2. Ingest Raw XML Data
df_raw = spark.read \
    .format("xml") \
    .option("rowTag", "measValue") \
    .load(S3_RAW_PATH)

print("Unpacking 3GPP nested XML schema...")

metrics_array = split(col("measResults"), " ")

df_extracted = df_raw.select(
    regexp_extract(col("_measObjLdn"), r"CellID=(.*)", 1).alias("cell_id"),
    # THE FIX: Replace the 'T' from the ISO string so PySpark can parse it
    regexp_replace(col("suspectFlag"), "T", " ").cast("timestamp").alias("collection_timestamp"),
    metrics_array.getItem(0).cast("float").alias("dl_prb_utilization_pct"),
    metrics_array.getItem(1).cast("float").alias("ul_prb_utilization_pct"),
    metrics_array.getItem(2).cast("float").alias("dl_throughput_mbps"),
    metrics_array.getItem(3).cast("int").alias("active_ue_count"),
    metrics_array.getItem(4).cast("float").alias("rrc_connected_avg"),
    metrics_array.getItem(5).cast("int").alias("volte_sessions_active"),
    metrics_array.getItem(6).cast("float").alias("handover_success_rate"),
    metrics_array.getItem(7).cast("float").alias("cqi_mean"),
    metrics_array.getItem(8).cast("float").alias("sinr_mean_db"),
    metrics_array.getItem(9).cast("float").alias("availability_pct")
)

print("Enriching telemetry with Reference Topology (Market, Technology, Frequency Band)...")
df_enriched = df_extracted.join(broadcast(df_topo_enrich), "cell_id", "left")

print("Reconstructing derived architectural columns...")
df_parsed = df_enriched \
    .withColumn("enb_id", expr("concat('ENB', lpad(cast(cast(regexp_extract(cell_id, '\\\\d+', 0) as int) / 3 as int), 5, '0'))")) \
    .withColumn("collection_date", to_date(col("collection_timestamp")))

# 3. The Data Quality Rules Engine
print("Evaluating Data Quality Rules...")

dq_condition = (
    col("cell_id").isNotNull() &
    col("cell_id").rlike("^CELL[0-9]+$") & 
    (col("dl_prb_utilization_pct") >= 0.0) &
    (col("dl_prb_utilization_pct") <= 100.0) &
    (col("availability_pct") >= 0.0) &
    (col("availability_pct") <= 100.0)
)

# 4. The Routing Fork (Silver vs. Quarantine)
print("Routing records...")

df_clean = df_parsed.filter(dq_condition)

df_quarantine = df_parsed.filter(~dq_condition).withColumn(
    "dq_failure_reason",
    when(col("cell_id").isNull(), "CRITICAL: Null cell_id")
    .when(~col("cell_id").rlike("^CELL[0-9]+$"), "FORMAT: Invalid cell_id structure")
    .when((col("dl_prb_utilization_pct") < 0.0) | (col("dl_prb_utilization_pct") > 100.0), "ANOMALY: PRB utilization out of logical bounds")
    .when((col("availability_pct") < 0.0) | (col("availability_pct") > 100.0), "ANOMALY: Availability out of logical bounds")
    .otherwise("UNKNOWN: Multiple DQ rule failures")
)

# 5. Write to Apache Iceberg
print("Formatting schema to match ohana.pm_curated exactly...")

df_curated = df_clean.select(
    col("cell_id"),
    col("enb_id"),
    col("collection_date"),
    col("collection_timestamp"),
    col("market"),
    col("technology"),
    col("dl_prb_utilization_pct"),
    col("ul_prb_utilization_pct"),
    col("dl_throughput_mbps"),
    col("active_ue_count"),
    col("rrc_connected_avg"),
    col("availability_pct"),
    (col("availability_pct") < 100.0).alias("is_outage"),
    (col("dl_prb_utilization_pct") >= 80.0).alias("is_saturated"),
    lit(True).alias("dq_passed"),
    current_timestamp().alias("load_ts")
)

print("Writing validated records to ohana.pm_curated...")
# REPARTITION FIX: Group the 120,000 tiny files into optimized daily partitions
df_curated.repartition("collection_date").writeTo("ohana.pm_curated") \
    .using("iceberg") \
    .append()

print("Writing anomalous records to ohana.dq_quarantine...")
df_quarantine_final = df_quarantine.select(
    "cell_id", "collection_timestamp", "dl_prb_utilization_pct",
    "ul_prb_utilization_pct", "dl_throughput_mbps", "active_ue_count",
    "rrc_connected_avg", "volte_sessions_active", "handover_success_rate",
    "cqi_mean", "sinr_mean_db", "availability_pct", "frequency_band", 
    "enb_id", "dq_failure_reason"
)

# THE FIX: Replaced coalesce(1) with repartition(4)
df_quarantine_final.repartition(4).writeTo("ohana.dq_quarantine") \
    .using("iceberg") \
    .append()

print("✨ Batch Data Quality Pipeline Complete: Historical data successfully lakehoused.")
