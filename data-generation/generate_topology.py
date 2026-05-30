import pandas as pd, numpy as np, random, json, boto3, io
from datetime import datetime

np.random.seed(42); random.seed(42)
S3_BUCKET = 'ps-amer-ohana-telecom'   # replace with your actual bucket name

# ── Market definitions with realistic geographic bounding boxes ───────────
MARKETS = {
    'Honolulu':  {'lat': (21.25, 21.45), 'lon': (-157.95, -157.75), 'cells': 600,  'venues': 6},
    'Phoenix':   {'lat': (33.30, 33.70), 'lon': (-112.20, -111.80), 'cells': 1200, 'venues': 12},
    'Seattle':   {'lat': (47.45, 47.75), 'lon': (-122.45, -122.15), 'cells': 900,  'venues': 9},
    'Miami':     {'lat': (25.65, 25.90), 'lon': (-80.35, -80.10), 'cells': 1100, 'venues': 11},
    'Chicago':   {'lat': (41.65, 42.05), 'lon': (-87.90, -87.55), 'cells': 1200, 'venues': 12},
}
TECHNOLOGIES  = ['LTE', 'LTE', 'LTE', 'NR', 'NR-NSA']   # weighted: 60% LTE
BANDS         = {'LTE': ['B13','B66','B4'], 'NR': ['n77','n260'], 'NR-NSA': ['n41','n66']}
VENUE_TYPES   = ['Rural', 'Suburban', 'Urban', 'Dense Urban', 'Special Venue']
VENUE_WEIGHTS = [0.10, 0.35, 0.35, 0.15, 0.05]
CAPACITIES    = {'Rural': 75.0, 'Suburban': 150.0, 'Urban': 300.0,
                 'Dense Urban': 600.0, 'Special Venue': 1200.0}

sites, cells, neighbors = [], [], []
site_id = cell_id = 1

for market, cfg in MARKETS.items():
    n_sites  = cfg['cells'] // 3           # ~3 sectors per site
    n_venues = cfg['venues']
    venue_site_ids = set(random.sample(range(site_id, site_id + n_sites), n_venues))

    for _ in range(n_sites):
        lat  = np.random.uniform(*cfg['lat'])
        lon  = np.random.uniform(*cfg['lon'])
        is_v = site_id in venue_site_ids
        vtype = 'Special Venue' if is_v else np.random.choice(VENUE_TYPES[:-1], p=[0.10,0.37,0.37,0.16])
        vid  = f'VEN{site_id:04d}' if is_v else None

        for sector in range(3):               # 3 sectors: 0°, 120°, 240°
            tech = random.choice(TECHNOLOGIES)
            band = random.choice(BANDS[tech])
            cells.append({
                'site_id':              f'SITE{site_id:05d}',
                'cell_id':              f'CELL{cell_id:06d}',
                'enb_id':               f'ENB{site_id:05d}',
                'market':               market,
                'latitude':             round(lat + np.random.normal(0, 0.001), 6),
                'longitude':            round(lon + np.random.normal(0, 0.001), 6),
                'sector_azimuth_degrees':int(sector * 120),
                'antenna_height_m':      round(np.random.uniform(25, 65), 1),
                'carrier_technology':    tech,
                'carrier_band':          band,
                'max_dl_capacity_mbps':  CAPACITIES[vtype],
                'venue_type':            vtype,
                'special_venue_id':      vid,
                'cluster':               f'{market}-C{(site_id % 12)+1:02d}',
            })
            cell_id += 1
        site_id += 1

topo_df = pd.DataFrame(cells)
print(f'Topology: {len(topo_df):,} cell sectors · {topo_df.special_venue_id.notna().sum()} special venue cells')

# Build neighbor table: each cell has 6 nearest neighbors by lat/lon distance
from sklearn.neighbors import BallTree
coords = np.radians(topo_df[['latitude','longitude']].values)
tree   = BallTree(coords, metric='haversine')
_, idxs = tree.query(coords, k=7)   # k=7 includes self
for i, row in topo_df.iterrows():
    for j in idxs[i][1:]:              # skip self (index 0)
        neighbors.append({
            'cell_id':          row['cell_id'],
            'neighbor_cell_id': topo_df.iloc[j]['cell_id'],
        })
nbr_df = pd.DataFrame(neighbors)

# Write to S3
s3 = boto3.client('s3')
for df, key in [(topo_df, 'reference/topology/topology.csv'),
                 (nbr_df, 'reference/neighbors/cell_neighbors.csv')]:
    buf = io.StringIO(); df.to_csv(buf, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f'✓ Wrote s3://{S3_BUCKET}/{key}  ({len(df):,} rows)')
