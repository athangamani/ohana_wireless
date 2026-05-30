import pandas as pd
import numpy as np
import random
import boto3
import io
from datetime import datetime, timedelta

np.random.seed(99)
random.seed(99)

S3_BUCKET = 'ps-amer-ohana-telecom'

# 1. Initialize the Boto3 client (Confirmed Working)
s3 = boto3.client('s3')

# 2. Read the CSV using Boto3 to bypass the s3fs credential bug
print("Fetching topology from S3 via Boto3...")
response = s3.get_object(Bucket=S3_BUCKET, Key='reference/topology/topology.csv')
topo = pd.read_csv(response['Body'])

print("Pandas successfully loaded the dataframe!")
print(f"Total Rows: {len(topo)}\n")

# Get venues for the event generation
venues = topo[topo['special_venue_id'].notna()]['special_venue_id'].unique().tolist()

START_DATE = datetime(2023, 1, 1)    # 24-month window: Jan 2023 – Dec 2024
END_DATE   = datetime(2024, 12, 31)

CATEGORIES = {
    'NFL':       {'duration_h': 4,   'attendance': (55000, 75000), 'surge': 3.5},
    'NBA':       {'duration_h': 3,   'attendance': (18000, 24000), 'surge': 2.8},
    'Concert':   {'duration_h': 3,   'attendance': (10000, 50000), 'surge': 2.5},
    'Marathon':  {'duration_h': 6,   'attendance': (5000,  35000), 'surge': 1.8},
    'Emergency': {'duration_h': 8,   'attendance': (1000,  8000),  'surge': 4.5},
    'Other':     {'duration_h': 2,   'attendance': (1000,  5000),  'surge': 1.4},
}
CAT_WEIGHTS = [0.25, 0.20, 0.25, 0.12, 0.05, 0.13]   # 500 events total
CAT_KEYS    = list(CATEGORIES.keys())

events = []
for i in range(500):
    cat  = np.random.choice(CAT_KEYS, p=CAT_WEIGHTS)
    cfg  = CATEGORIES[cat]
    days = (END_DATE - START_DATE).days
    start_dt = START_DATE + timedelta(days=random.randint(0, days),
                                      hours=random.randint(10, 20))
    end_dt   = start_dt + timedelta(hours=cfg['duration_h'])
    venue    = random.choice(venues)
    attend   = random.randint(*cfg['attendance'])
    
    events.append({
        'event_id':                  f'EVT{i+1:04d}',
        'venue_id':                  venue,
        'event_name':                f'Synthetic {cat} Event {i+1}',
        'event_category':            cat,
        'expected_attendance':       attend,
        'event_start_ts':            start_dt.isoformat(),
        'event_end_ts':              end_dt.isoformat(),
        'surge_multiplier_estimate': round(cfg['surge'] + np.random.normal(0, 0.3), 2),
    })

events_df = pd.DataFrame(events)

# 3. Write the JSON directly to S3 using Boto3
print("Writing events_calendar.json back to S3...")
json_buffer = io.StringIO()
events_df.to_json(json_buffer, orient='records', indent=2)

s3.put_object(
    Bucket=S3_BUCKET, 
    Key='reference/events/events_calendar.json', 
    Body=json_buffer.getvalue()
)

print(f'✓ Generated {len(events_df):,} events across {len(venues)} special venues')
print(events_df['event_category'].value_counts().to_string())
