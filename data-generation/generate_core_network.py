import pandas as pd, numpy as np, boto3, io, gzip
from datetime import datetime, timedelta

S3_BUCKET = 'ps-amer-ohana-telecom'

np.random.seed(77)
NODES  = [f'CORE{i:03d}' for i in range(1, 21)]     # 20 nodes
TYPES  = ['SGW', 'PGW', 'UPF', 'AMF', 'SMF']
REGS   = ['Northeast', 'Southeast', 'Midwest', 'West', 'Southwest']

# Node static config
node_cfg = [{'node_id': n, 'node_type': TYPES[i%5], 'region': REGS[i%5]}
            for i, n in enumerate(NODES)]

START = datetime(2023, 1, 1)
rows  = []
ts    = START
while ts < datetime(2025, 1, 1):
    hour_factor = 1.0 + 0.6 * np.sin((ts.hour - 3) * np.pi / 12)
    dow_factor  = 0.75 if ts.weekday() >= 5 else 1.0

    for cfg in node_cfg:
        base_tput = {'SGW':18.0,'PGW':22.0,'UPF':35.0,'AMF':8.0,'SMF':12.0}[cfg['node_type']]
        s1u   = max(0.1, base_tput * hour_factor * dow_factor + np.random.normal(0, 1.5))
        cpu   = min(99, s1u / base_tput * 60 + np.random.normal(0, 5))
        rows.append({
            'node_id':                 cfg['node_id'],
            'node_type':               cfg['node_type'],
            'collection_timestamp':    ts.isoformat(),
            'region':                  cfg['region'],
            'active_bearers':          int(s1u * 280 + np.random.normal(0, 200)),
            's1u_throughput_gbps':     round(s1u, 4),
            'pgw_upf_cpu_utilization_pct': round(max(0, min(99, cpu)), 2),
            'pgw_upf_session_count':   int(s1u * 1400),
            'dl_packet_drop_rate':     round(max(0, np.random.exponential(0.002)), 5),
            'ul_packet_drop_rate':     round(max(0, np.random.exponential(0.001)), 5),
            'average_latency_ms':      round(max(5, np.random.normal(28, 6)), 2),
            'gtp_tunnel_count':        int(s1u * 960),
        })
    ts += timedelta(minutes=15)

core_df = pd.DataFrame(rows)
print(f'Core network: {len(core_df):,} rows ({len(core_df)/1e6:.1f}M)')

# Write as partitioned CSV to S3 (one file per month per region)
core_df['month'] = pd.to_datetime(core_df['collection_timestamp']).dt.to_period('M').astype(str)
for month, grp in core_df.groupby('month'):
    key = f'raw/core-probe/{month}/core_network_{month}.csv.gz'
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        gz.write(grp.drop(columns=['month']).to_csv(index=False).encode())
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
print(f'✓ Core network written to s3://{S3_BUCKET}/raw/core-probe/')
