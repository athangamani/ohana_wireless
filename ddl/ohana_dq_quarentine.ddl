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
