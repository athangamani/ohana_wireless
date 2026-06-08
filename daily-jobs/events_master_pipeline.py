from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
from pyspark.sql.types import IntegerType, TimestampType, FloatType
import pandas as pd
import numpy as np
import random
import boto3
from datetime import datetime, timedelta

# Initialize Spark
spark = SparkSession.builder.appName('ohana-master-events-pipeline').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET = 'ps-amer-ohana-telecom'

# ---------------------------------------------------------
# STEP 1: GENERATE SYNTHETIC EVENTS (Native Python)
# ---------------------------------------------------------
print("Fetching topology from S3 via Boto3...")
s3 = boto3.client('s3')
response = s3.get_object(Bucket=S3_BUCKET, Key='reference/topology/topology.csv')
topo = pd.read_csv(response['Body'])

print("Pandas successfully loaded the topology dataframe!")

venues = topo[topo['special_venue_id'].notna()]['special_venue_id'].unique().tolist()

# Set start date to TODAY so the NOC dashboard sees upcoming events
START_DATE = datetime.now()
END_DATE   = START_DATE + timedelta(days=30) 

CATEGORIES = {
    'NFL':       {'duration_h': 4,   'attendance': (55000, 75000), 'surge': 3.5},
    'NBA':       {'duration_h': 3,   'attendance': (18000, 24000), 'surge': 2.8},
    'Concert':   {'duration_h': 3,   'attendance': (10000, 50000), 'surge': 2.5},
    'Marathon':  {'duration_h': 6,   'attendance': (5000,  35000), 'surge': 1.8},
    'Emergency': {'duration_h': 8,   'attendance': (1000,  8000),  'surge': 4.5},
    'Other':     {'duration_h': 2,   'attendance': (1000,  5000),  'surge': 1.4},
}
CAT_WEIGHTS = [0.25, 0.20, 0.25, 0.12, 0.05, 0.13]
CAT_KEYS    = list(CATEGORIES.keys())

events = []
# Generate exactly 15 events for the daily run
for i in range(15):
    cat  = np.random.choice(CAT_KEYS, p=CAT_WEIGHTS)
    cfg  = CATEGORIES[cat]
    days = (END_DATE - START_DATE).days
    start_dt = START_DATE + timedelta(days=random.randint(0, days),
                                      hours=random.randint(10, 20))
    end_dt   = start_dt + timedelta(hours=cfg['duration_h'])
    venue    = random.choice(venues)
    attend   = random.randint(*cfg['attendance'])
    
    # Generate a unique ID so it merges cleanly without overwriting history
    unique_id = f'EVT-{start_dt.strftime("%Y%m%d")}-{i:02d}'
    
    events.append({
        'event_id':                  unique_id,
        'venue_id':                  venue,
        'event_name':                f'Synthetic {str(cat)} Event {i+1}', # <-- Cast here
        'event_category':            str(cat),                            # <-- Cast here
        'expected_attendance':       attend,
        'event_start_ts':            start_dt.isoformat(),
        'event_end_ts':              end_dt.isoformat(),
        'surge_multiplier_estimate': round(cfg['surge'] + np.random.normal(0, 0.3), 2),
    })

print(f"Generated {len(events)} daily events.")

# ---------------------------------------------------------
# STEP 2: CONVERT TO PYSPARK & CAST SCHEMA
# ---------------------------------------------------------
df = spark.createDataFrame(events)

# Cast to exact Iceberg schema types
df_cast = df.select(
    col("event_id"),
    col("venue_id"),
    col("event_name"),
    col("event_category"),
    col("expected_attendance").cast(IntegerType()),
    col("event_start_ts").cast(TimestampType()),
    col("event_end_ts").cast(TimestampType()),
    col("surge_multiplier_estimate").cast(FloatType())
)

# ---------------------------------------------------------
# STEP 3: WRITE TO S3 RAW ARCHIVE (USING BOTO3)
# ---------------------------------------------------------
import io

s3_raw_key = f"raw/events/{START_DATE.strftime('%Y/%m/%d')}/daily_events.json"
print(f"Writing raw JSON to S3 landing zone using Boto3: {s3_raw_key}")

# Convert the Spark DataFrame to Pandas, then to a JSON string buffer
pdf = df_cast.toPandas()
json_buffer = io.StringIO()
pdf.to_json(json_buffer, orient='records', indent=2)

# Use boto3 to put the object, bypassing Ranger RAZ
s3.put_object(
    Bucket=S3_BUCKET, 
    Key=s3_raw_key, 
    Body=json_buffer.getvalue()
)

# ---------------------------------------------------------
# STEP 4: WRITE TO KAFKA (REAL-TIME PIPELINE)
# ---------------------------------------------------------
print("Publishing events to Kafka topic...")
# Kafka needs a single string column named 'value'
kafka_df = df_cast.select(to_json(struct("*")).alias("value"))

KAFKA_BROKERS = "ps-amer-kafka-hub-corebroker0.ps-amer.a465-9q4k.cloudera.site:9093"
KAFKA_TOPIC = "network.events.raw"
# IMPORTANT: Update with your actual workload password
JAAS_CONFIG = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="athangamani" password="BlancaLake123";'

kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", KAFKA_TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .save()

# ---------------------------------------------------------
# STEP 5: MERGE INTO ICEBERG (BATCH PIPELINE)
# ---------------------------------------------------------
print("Upserting (MERGE) into Iceberg table...")
df_cast.createOrReplaceTempView("daily_events_updates")

merge_query = """
MERGE INTO ohana.events_calendar t
USING daily_events_updates s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
    t.expected_attendance = s.expected_attendance,
    t.event_start_ts = s.event_start_ts,
    t.event_end_ts = s.event_end_ts,
    t.surge_multiplier_estimate = s.surge_multiplier_estimate
WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(merge_query)

print("✨ Pipeline Complete: Daily events generated, archived, lakehoused, and published!")
