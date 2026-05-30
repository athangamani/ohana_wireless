CREATE TABLE IF NOT EXISTS ohana.topology ( 
    site_id STRING, 
    cell_id STRING, 
    enb_id STRING, 
    market STRING, 
    latitude DOUBLE, 
    longitude DOUBLE, 
    sector_azimuth_degrees INT, 
    antenna_height_m FLOAT, 
    carrier_technology STRING, 
    carrier_band STRING, 
    max_dl_capacity_mbps FLOAT, 
    venue_type STRING, 
    special_venue_id STRING, 
    cluster STRING ) 
STORED AS ICEBERG 
TBLPROPERTIES ('format-version'='2');
