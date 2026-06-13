# ohana_wireless

In CAI environment before data generation, create a python project template and change cdsw.sh  

pip install scikit-learn  
pip install fsspec s3fs  
pip install --upgrade boto3 botocore s3fs aiobotocore fsspec  


In CDE environment for data generation build an environment  

```bash
jupyter-arun@ip-10-0-0-183:~$ vi requirements.txt  
pandas 
pyarrow  
boto3  
  
jupyter-arun@ip-10-0-0-183:$ cde resource create --name pm-pandas-env --type python-env
jupyter-arun@ip-10-0-0-183:$ cde resource upload --name pm-pandas-env --local-path requirements.txt
```
  
I am taking a shortcut here.. Here is what the capstone project says..   

Run this once per day.  
http website for events ←→ Create a nifi flow to pull from.  
Nifi flow → write to s3.  
Nifi flow → write to kafka topic.  
S3 → CDE job → merge into iceberg table.  

Since this is just a reference table i am going to do all of this with a CDE job.  
Run this once per day.  - events_master_pipeline.py.  
CDE Job.  
Generate 15 events.  
Write to s3.  -- details below. 
Write to kafka topic - network.events.raw.  
Convert to write and merge into iceberg table.   
So we generated 15 events and wrote it to s3.  
Writing raw JSON to S3 landing zone: s3a://ps-amer-ohana-telecom/raw/events/2026/05/21/.  
The original 500 events are in.  
s3://ps-amer-ohana-telecom/reference/events/events_calendar.json.  


Now setting up the nifi flow to read ftp data from the ubuntu box
ssh ubuntu@35.91.66.114
Password Ohana2026!
ubuntu@ip-10-0-0-183:~$ sudo passwd ubuntu
New password: 
Retype new password: 
passwd: password updated successfully
ubuntu@ip-10-0-0-183:~$ sudo nano /etc/ssh/sshd_config
ubuntu@ip-10-0-0-183:~$ sudo systemctl restart ssh
PasswordAuthentication yes
ubuntu@ip-10-0-0-183:~$ Read from remote host 35.91.66.114: No route to host
Connection to 35.91.66.114 closed.
client_loop: send disconnect: Broken pipe
arunthangamani@G5LFC2PC9W tower-data-generator % ssh ubuntu@35.91.66.114
ubuntu@35.91.66.114: Permission denied (publickey).

ssh -i ~/Downloads/ec2-machine.pem ubuntu@35.91.66.114
ls -l /etc/ssh/sshd_config.d/
sudo nano /etc/ssh/sshd_config.d/50-cloud-init.conf
Inside that file, you will almost certainly see this line:
PasswordAuthentication no
Change it to:
PasswordAuthentication yes
Press Ctrl+O, then Enter to save. Press Ctrl+X to exit.
Step 4: Restart the SSH Service again
sudo systemctl restart ssh
ssh ubuntu@35.91.66.114
Ohana2026!

historically how the tables got filled up.  
generate topology.  
generate core network.   
generate events    
   and then generate events became a single job that generated events wrote into iceberg table, kafka and s3    folder (events-master-pipeline.py).    
we first generated a raw-pm s3 folders that had the big xml messages - generate random-pm-data.   
then we did load-data-to-tables.   
    load-topology.   
    load-neighbors.   
    load-events.   
    etl_pm_curated.py.    
we generated ml_feature_store using feature_engineering.   
then we backfilled ml_feature_store with events using ml_feature_engineering_historical_backfill.   

then see the demo steps to setup the demo.    
also there is a nightly job that in under airflow folder which runs 3 jobs    
    events_master_pipeline.py.   
    feature_engineering_incremental.py.  
    kpi_sliding_window.py - the kpi metrics is for dashboards we did not use it anywhere.  
    to load the nightly job here are the commands i used.  
        240  cde resource upload --name ohana-ml-scripts --local-path daily_pipeline.py. 
        241  cde resource upload --name ohana-ml-scripts --local-path kpi_sliding_window.py. 
        242  cde resource upload --name ohana-ml-scripts --local-path feature_engineering_incremental.py. 
        243  cde job run --name ohana_nightly_ml_pipeline  
        244  cde resource upload --name ohana-ml-scripts --local-path daily_pipeline.py.  
        245  cde resource upload --name ohana-ml-scripts --local-path kpi_sliding_window.py.  
        246  cde resource upload --name ohana-ml-scripts --local-path feature_engineering_incremental.py.  
        247  cde job run --name ohana_nightly_ml_pipeline.  
        
        249  cde resource upload --name ohana-ml-scripts --local-path feature_engineering_incremental.py.  
        250  cde resource upload --name ohana-ml-scripts --local-path kpi_sliding_window.py.  
        251  cde job run --name ohana_nightly_ml_pipeline.  
        252  cde resource upload --name ohana-ml-scripts --local-path daily_pipeline.py.  
        253  less daily_pipeline.py    
        254  cde job run --name ohana_nightly_ml_pipeline.   
        255  cde job update --name feature_engineering_incremental_job   --arg "--execution_date"   --arg "{{{ execution_date }}}".    
        256  cde job update --name kpi_sliding_window_job   --arg "--execution_date"   --arg "{{{ execution_date }}}".   
        257  cde resource upload --name ohana-ml-scripts --local-path daily_pipeline.py.   
        258  cde resource upload --name ohana-ml-scripts --local-path kpi_sliding_window.py.  
        259  cde resource upload --name ohana-ml-scripts --local-path feature_engineering_incremental.py.  
        260  cde job run --name ohana_nightly_ml_pipeline.  
        261  cde resource upload --name ohana-ml-scripts --local-path feature_engineering_incremental.py.    
        262  cde resource upload --name ohana-ml-scripts --local-path kpi_sliding_window.py.   
        263  cde job run --name ohana_nightly_ml_pipeline.   

demo steps.  
0) train and deploy the demo. 
0) start the nifi job to write to ftp server and kafka topic.  
0) start the processing_cell_tower_telemetry.py job to read from kafka topic and write to impala.  
1) inject mega event to trigger the alert.  
2) run the flush script to simulate the flush phase with 90% DL PRB utilization.  
3) check impala to see data from tower    
-- Force Impala to see the new records    
INVALIDATE METADATA ohana.live_inference_stream;   
-- Verify it is sitting at the very top of the table.   
SELECT    
    window_start,    
    rolling_ul_utilization_pct   
FROM ohana.live_inference_stream   
WHERE cell_id = 'CELL001801'    
ORDER BY window_start DESC   
LIMIT 3;  
4) check the model api to see the alert firing   
5) run the recovery cool-off script to simulate the recovery phase with 0% DL PR.   
6) check the model api to see the alert recovery    
 