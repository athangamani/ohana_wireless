from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Initialize CDE Spark Session (Iceberg is natively supported here)
spark = SparkSession.builder.appName("Ohana-Inject-Mega-Event").getOrCreate()

# Calculate the timestamps dynamically
now = datetime.now()
start_ts = (now - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
end_ts = (now + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')

print(f"Injecting Kukui Chaos Mega-Concert from {start_ts} to {end_ts}...")

# Change the values inside the INSERT statement to match the production keys:
spark.sql(f"""
    INSERT INTO ohana.events_calendar VALUES (
        'MOCK-EVNT-01', 
        'VEN0601', 
        'Kukui Chaos Mega-Concert', 
        'Concert', 
        55000, 
        CAST('{start_ts}' AS TIMESTAMP), 
        CAST('{end_ts}' AS TIMESTAMP),
        2.5
    )
""")

print("✅ Mega-Concert successfully registered in Iceberg via CDE!")