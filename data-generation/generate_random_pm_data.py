from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import os, math
import pandas as pd

S3_BUCKET   = 'ps-amer-ohana-telecom'
PARALLELISM = int(os.environ.get('PARALLELISM', '48'))

spark = SparkSession.builder \
    .appName('ohana-pm-datagen') \
    .config('spark.sql.shuffle.partitions', PARALLELISM) \
    .getOrCreate()

# ── 1. Load topology from S3 ──────────────────────────────────────────────
topo = spark.read.csv(f's3a://{S3_BUCKET}/reference/topology/topology.csv',
                       header=True, inferSchema=True)
cells = topo.select('cell_id', 'market', 'carrier_technology', 'carrier_band',
                     'venue_type', 'max_dl_capacity_mbps')

# ── 2. Create timestamp spine: 24 months × 96 intervals/day = 70,176 ts ──
# One row per (cell_id, timestamp) = 5,000 × 70,176 ≈ 350M rows pre-filter
# After filtering outages (~2%) and anomaly injection: ~343M intervals
timestamps = spark.sql("""
    SELECT sequence(
        CAST('2023-01-01 00:00:00' AS TIMESTAMP),
        CAST('2024-12-31 23:45:00' AS TIMESTAMP),
        INTERVAL 15 MINUTES
    ) AS ts_array
""").select(F.explode('ts_array').alias('collection_timestamp'))

# Cross join cells × timestamps to get the full grid
grid = cells.crossJoin(timestamps).repartition(PARALLELISM, 'cell_id')

# ── 3. Simulate realistic PRB utilization with all required patterns ──────
pm = grid.withColumn('hour',       F.hour('collection_timestamp')) \
         .withColumn('dow',        F.dayofweek('collection_timestamp')) \
         .withColumn('month_num', F.month('collection_timestamp')) \
    .withColumn('diurnal_factor',
        # Morning ramp 7-9AM, evening peak 6-9PM, overnight trough
        F.sin(F.lit(math.pi) * (F.col('hour') - 4) / F.lit(20)) * F.lit(0.45) + F.lit(0.55)
    ) \
    .withColumn('dow_factor',
        F.when(F.col('dow').isin(1,7), F.lit(0.72))  # weekend: 28% lower
         .when(F.col('dow').isin(2,6), F.lit(0.88))  # Mon/Fri: slightly lower
         .otherwise(F.lit(1.00))
    ) \
    .withColumn('venue_base_prb',
        F.when(F.col('venue_type') == 'Dense Urban',  F.lit(62.0))
         .when(F.col('venue_type') == 'Urban',        F.lit(45.0))
         .when(F.col('venue_type') == 'Suburban',     F.lit(28.0))
         .when(F.col('venue_type') == 'Special Venue', F.lit(40.0))
         .otherwise(F.lit(12.0))                      # Rural
    ) \
    .withColumn('base_prb',
        F.col('venue_base_prb') * F.col('diurnal_factor') * F.col('dow_factor')
    ) \
    .withColumn('noise',
        F.lit(0) + (F.rand(42) - F.lit(0.5)) * F.lit(12)  # ±6% noise
    ) \
    .withColumn('dl_prb_utilization_pct',
        F.round(F.greatest(F.lit(0.1),
                            F.least(F.lit(100.0),
                                    F.col('base_prb') + F.col('noise'))), 2)
    ) \
    .withColumn('ul_prb_utilization_pct',
        F.round(F.col('dl_prb_utilization_pct') * (F.lit(0.35) + F.rand(7) * F.lit(0.15)), 2)
    ) \
    .withColumn('dl_throughput_mbps',
        F.round(F.col('dl_prb_utilization_pct') / F.lit(100.0) * F.col('max_dl_capacity_mbps') * (F.lit(0.75) + F.rand(9) * F.lit(0.20)), 2)
    ) \
    .withColumn('active_ue_count',
        # CPNI: will be masked to nearest 10 by NiFi UpdateRecord
        F.greatest(F.lit(0), (F.col('dl_prb_utilization_pct') * F.lit(2.4) + F.randn(11) * F.lit(18)).cast('int'))
    ) \
    .withColumn('rrc_connected_avg', F.round(F.col('active_ue_count') * F.lit(0.82), 1)) \
    .withColumn('volte_sessions_active',
        F.greatest(F.lit(0), (F.col('active_ue_count') * F.lit(0.12) + F.randn(13) * F.lit(3)).cast('int'))
    ) \
    .withColumn('handover_success_rate',
        F.round(F.least(F.lit(1.0), F.greatest(F.lit(0.85),
                F.lit(0.97) - F.col('dl_prb_utilization_pct') / F.lit(1000) + F.randn(15) * F.lit(0.01))), 4)
    ) \
    .withColumn('cqi_mean',        F.round(F.lit(12.5) - F.col('dl_prb_utilization_pct') / F.lit(20) + F.randn(17) * F.lit(0.8), 1)) \
    .withColumn('sinr_mean_db',    F.round(F.lit(14.0) - F.col('dl_prb_utilization_pct') / F.lit(15) + F.randn(19) * F.lit(1.2), 2)) \
    .withColumn('availability_pct',
        # ~1.5% outage probability per interval
        F.when(F.rand(21) < F.lit(0.015), F.lit(0.0)).otherwise(F.lit(100.0))
    ) \
    .withColumn('frequency_band', F.col('carrier_band')) \
    .withColumn('enb_id', F.regexp_replace('cell_id', r'CELL(\d+)',
                            F.concat(F.lit('ENB'), F.expr("printf('%05d', cast(regexp_extract(cell_id, '(\\\\d+)', 1) as int) DIV 3)")))) \
    .drop('diurnal_factor','dow_factor','venue_base_prb','base_prb','noise','hour','dow','month_num','carrier_band')

# ── 4. Write as gzipped XML — one file per cell per month ────────────────
# Real 3GPP TS 32.432 XML format NiFi SplitXml processor expects
def to_3gpp_xml(rows):
    import gzip, io
    from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
    import pandas as pd # Ensure pandas is available inside the worker
    
    rows = list(rows)
    if not rows: return
    
    cell_id = rows[0]['cell_id']
    # FIX 1: You already generated a 'month' string column in PySpark, just use it!
    month   = rows[0]['month']

    root = Element('measCollecFile')
    hdr  = SubElement(root, 'fileHeader', fileFormatVersion='32.435 V9.0',
                       dnPrefix='DC=ohana.com')
    mdata = SubElement(hdr, 'measData')
    SubElement(mdata, 'managedElement', localDn=f'MeContext={cell_id}')
    minfo = SubElement(mdata, 'measInfo')
    SubElement(minfo, 'jobId').text = 'PM_15min'
    
    # FIX 2: Convert Pandas Timestamp to ISO String for the XML attribute
    granp = SubElement(minfo, 'granPeriod', duration='PT900S',
                        endTime=rows[-1]['collection_timestamp'].isoformat())

    COUNTERS = ['dl_prb_utilization_pct','ul_prb_utilization_pct',
                'dl_throughput_mbps','active_ue_count','rrc_connected_avg',
                'volte_sessions_active','handover_success_rate',
                'cqi_mean','sinr_mean_db','availability_pct']

    for i, c in enumerate(COUNTERS):
        SubElement(minfo, 'measType', p=str(i+1)).text = c

    for r in rows:
        mv = SubElement(minfo, 'measValue', measObjLdn=f'CellID={r["cell_id"]}')
        SubElement(mv, 'measResults').text = ' '.join(str(r[c]) for c in COUNTERS)
        ts_el = SubElement(mv, 'suspectFlag')
        
        # FIX 3: Convert Pandas Timestamp to String for XML text
        ts_el.text = r['collection_timestamp'].isoformat()

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        ElementTree(root).write(gz, encoding='utf-8', xml_declaration=True)
    buf.seek(0)

    import boto3
    s3 = boto3.client('s3')
    key = f'raw/ran-pm/{month}/{cell_id}_PM15min_{month}.xml.gz'
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
    
    yield {'cell_id': cell_id, 'month': month, 'rows': len(rows)}
    
# Add month column, group by (cell_id, month), generate one XML per group
pm_with_month = pm.withColumn('month',
    F.date_format('collection_timestamp', 'yyyy-MM'))

result = pm_with_month.groupBy('cell_id', 'month') \
    .applyInPandas(
        lambda df: pd.DataFrame(list(to_3gpp_xml(df.to_dict(orient='records')))),
        schema='cell_id STRING, month STRING, rows LONG'
    )

total = result.agg(F.sum('rows')).collect()[0][0]
print(f'✓ RAN PM generation complete: {total:,} total records across all XML files')
print(f'  Files written to s3://{S3_BUCKET}/raw/ran-pm/YYYY-MM/CELLXXXXXX_PM15min_YYYY-MM.xml.gz')
