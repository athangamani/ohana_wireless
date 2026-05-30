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


DESCRIBE ohana.streaming_kpi_agg;

SELECT * FROM ohana.streaming_kpi_agg ORDER BY dl_prb_util_mean DESC LIMIT 10;

SELECT * FROM ohana.streaming_kpi_agg ORDER BY window_start DESC LIMIT 10;
