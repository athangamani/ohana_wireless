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