SELECT COUNT(*) FROM ohana.topology;
select * from ohana.topology limit 5;

SELECT COUNT(*) FROM ohana.events_calendar;
select * from ohana.events_calendar limit 5;

INVALIDATE METADATA ohana.cell_neighbors;

select count(*) from ohana.cell_neighbors;
select * from ohana.cell_neighbors limit 5;

SELECT market, COUNT(*) AS cell_sectors,
       COUNT(DISTINCT site_id) AS sites,
       SUM(CASE WHEN special_venue_id IS NOT NULL THEN 1 ELSE 0 END) AS venue_cells,
       COUNT(DISTINCT carrier_technology) AS tech_types
FROM   ohana.topology
GROUP BY market ORDER BY market;


SELECT event_category, COUNT(*) AS n_events,
       AVG(expected_attendance) AS avg_attendance,
       AVG(surge_multiplier_estimate) AS avg_surge
FROM   ohana.events_calendar
GROUP BY event_category ORDER BY n_events DESC;


-- Fact table: one row per cell sector per 15-min interval
CREATE TABLE IF NOT EXISTS ohana.pm_curated (
  cell_id                  STRING    NOT NULL,
  enb_id                   STRING    NOT NULL,
  collection_date          DATE      NOT NULL,
  collection_timestamp     TIMESTAMP NOT NULL,
  market                   STRING    NOT NULL,
  technology               STRING    NOT NULL,
  dl_prb_utilization_pct   FLOAT     NOT NULL,
  ul_prb_utilization_pct   FLOAT     NOT NULL,
  dl_throughput_mbps       FLOAT,
  active_ue_count          INT       NOT NULL,   -- already CPNI-masked to nearest 10
  rrc_connected_avg        FLOAT     NOT NULL,
  availability_pct         FLOAT     NOT NULL,
  is_outage                BOOLEAN   NOT NULL,   -- TRUE when availability_pct = 0
  is_saturated             BOOLEAN   NOT NULL,   -- TRUE when dl_prb_util = 100%
  dq_passed                BOOLEAN   NOT NULL,
  load_ts                  TIMESTAMP NOT NULL
)
PARTITIONED BY SPEC (collection_date, market)
STORED AS iceberg
TBLPROPERTIES (
  'format-version'                  = '2',
  'write.parquet.compression-codec' = 'snappy',
  'write.merge.mode'                = 'merge-on-read'  -- MOR: efficient for streaming appends
);



-- Streaming KPI aggregations: 5-minute windowed metrics
CREATE TABLE IF NOT EXISTS ohana.streaming_kpi_agg (
  cell_id              STRING    NOT NULL,
  market               STRING    NOT NULL,
  window_start         TIMESTAMP NOT NULL,
  window_end           TIMESTAMP NOT NULL,
  dl_prb_util_mean     FLOAT     NOT NULL,
  dl_prb_util_max      FLOAT     NOT NULL,
  dl_prb_util_p95      FLOAT     NOT NULL,
  active_ue_count_max  INT,
  dl_throughput_mean   FLOAT,
  anomaly_flag         BOOLEAN   NOT NULL
)
PARTITIONED BY SPEC (days(window_start), market)  -- hidden transform partition
STORED AS iceberg
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');


SELECT COUNT(*) FROM ohana.pm_curated;

DESCRIBE ohana.streaming_kpi_agg;

SELECT * FROM ohana.streaming_kpi_agg ORDER BY dl_prb_util_mean DESC LIMIT 10;

CREATE TABLE IF NOT EXISTS ohana.dq_quarantine (
    cell_id STRING,
    collection_timestamp TIMESTAMP,
    dl_prb_utilization_pct FLOAT,
    ul_prb_utilization_pct FLOAT,
    dl_throughput_mbps FLOAT,
    active_ue_count INT,
    rrc_connected_avg FLOAT,
    volte_sessions_active INT,
    handover_success_rate FLOAT,
    cqi_mean FLOAT,
    sinr_mean_db FLOAT,
    availability_pct FLOAT,
    frequency_band STRING,
    enb_id STRING,
    dq_failure_reason STRING
)
STORED BY ICEBERG
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

TRUNCATE TABLE ohana.pm_curated;
TRUNCATE TABLE ohana.dq_quarantine;

SELECT * FROM ohana.ml_feature_store 
WHERE market = 'Chicago' AND confidence_tier = 'HIGH' limit 10;
    
select count(*) from ohana.pm_curated;
select count(*) from ohana.ml_feature_store;

select * from ohana.ml_feature_store limit 5;
select * from ohana.ml_feature_store where active_surge_multiplier = 0;


describe ohana.ml_feature_store;

DROP TABLE IF EXISTS ohana.ml_feature_store;

CREATE TABLE ohana.ml_feature_store (
    cell_id STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    rolling_ul_utilization_pct DOUBLE,
    rolling_dl_throughput_mbps DOUBLE,
    rolling_active_ue_count DOUBLE
) USING ICEBERG;


DROP TABLE IF EXISTS ohana.live_inference_stream;

CREATE TABLE ohana.live_inference_stream (
    cell_id                     STRING      NOT NULL,
    window_start                TIMESTAMP   NOT NULL,
    window_end                  TIMESTAMP   NOT NULL,
    rolling_ul_utilization_pct  DOUBLE,
    rolling_dl_throughput_mbps  DOUBLE,
    rolling_active_ue_count     DOUBLE
) 
PARTITIONED BY SPEC (days(window_start), cell_id)
STORED AS iceberg
TBLPROPERTIES (
    'format-version' = '2', 
    'write.merge.mode' = 'merge-on-read'
);

SELECT * from ohana.live_inference_stream limit 5;

SELECT count(*) FROM ohana.ml_feature_store TABLESAMPLE SYSTEM(1)
    WHERE confidence_tier = 'HIGH'
      AND rand() <= 0.1

SELECT  
                rolling_ul_utilization_pct,
                rolling_dl_throughput_mbps,
                rolling_active_ue_count
            FROM ohana.live_inference_stream 
            WHERE cell_id = 'SEA-001'
            ORDER BY window_start DESC
            LIMIT 1
            
SELECT COUNT(*) 
FROM ohana.ml_feature_store;

SELECT COUNT(*) 
FROM ohana.live_inference_stream;

SELECT COUNT(*) 
FROM ohana.pm_curated;


    SELECT count(*) FROM ohana.ml_feature_store TABLESAMPLE SYSTEM(1)
    WHERE confidence_tier = 'HIGH'
      AND (
          -- Take a 10% sample of normal, non-event traffic
          (active_event_flag = 0 AND rand() <= 0.1)
          OR 
          -- Take 100% of the rare event traffic so the model can learn the surge patterns
          (active_event_flag = 1)
      )


SELECT 
    l.cell_id, 
    t.market,
    e.event_name,
    e.expected_attendance,
    l.rolling_ul_utilization_pct,
    l.rolling_active_ue_count,
    l.window_start
FROM ohana.live_inference_stream l
JOIN ohana.topology t ON l.cell_id = t.cell_id
JOIN ohana.events_calendar e ON t.special_venue_id = e.venue_id
WHERE now() >= e.event_start_ts AND now() <= e.event_end_ts
ORDER BY l.window_start DESC, l.rolling_ul_utilization_pct DESC
LIMIT 5;

INVALIDATE METADATA ohana.events_calendar;
INVALIDATE METADATA ohana.live_inference_stream;
INVALIDATE METADATA ohana.topology;

SELECT cell_id, special_venue_id 
FROM ohana.topology 
WHERE cell_id = 'CELL001801';

select count(*) from ohana.topology;
select * from ohana.topology where market = 'Seattle' limit 5;


INVALIDATE METADATA ohana.live_inference_stream;

SELECT window_start, rolling_ul_utilization_pct 
FROM ohana.live_inference_stream 
WHERE cell_id = 'CELL001801' 
ORDER BY window_start DESC 
LIMIT 3;

DELETE FROM ohana.live_inference_stream 
WHERE cell_id = 'CELL001801';
  
INSERT INTO ohana.live_inference_stream VALUES (
    'CELL001801',                   -- 1. cell_id
    now(),                          -- 2. window_start
    now() + interval 5 minutes,     -- 3. window_end
    98.75,                          -- 4. rolling_ul_utilization_pct (The Spike)
    4.5,                            -- 5. rolling_dl_throughput_mbps (Throttled)
    46500.0                         -- 6. rolling_active_ue_count    (Massive Crowd)
);

DESCRIBE ohana.live_inference_stream;
