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

throughout the process of using cde I used jupyterhub to edit code and submit to cde. 
jupyterhub login modifications are written in    
https://docs.google.com/document/d/1Ji0sWwCt97HoHRVlwZUpd4f5IV-S1uYXGTU3me2dM5g/edit?tab=t.0   

directory listing of the jupyter hub user arun.. who submitted all the jobs..    
jupyter-arun@ip-10-0-0-183:~$ ls 

jupyter-arun@ip-10-0-0-183:~$ ls -alt   
total 292.  
-rw-------  1 jupyter-arun jupyter-arun 41991 Jun 13 03:15 .bash_history.   
drwxr-x---  8 jupyter-arun jupyter-arun  4096 Jun 12 01:56 .   
-rw-------  1 jupyter-arun jupyter-arun    20 Jun 12 01:56 .lesshst.  
-rw-r--r--  1 jupyter-arun jupyter-arun 80889 Jun 12 01:55 generate_random_pm_data.ipynb.   
-rw-r--r--  1 jupyter-arun jupyter-arun  5106 Jun 12 01:53 processing_cell_tower_telemetry.py.   
drwxr-xr-x  2 jupyter-arun jupyter-arun  4096 Jun 10 22:09 .ipynb_checkpoints.    
-rw-r--r--  1 jupyter-arun jupyter-arun  5424 Jun 10 21:36 training_gbt_classifier_model.py.  
-rw-r--r--  1 jupyter-arun jupyter-arun  1093 Jun  9 05:24 inject_mega_event.py    
-rw-r--r--  1 jupyter-arun jupyter-arun  2976 Jun  8 20:06 kpi_sliding_window.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  7208 Jun  8 20:01 feature_engineering_incremental.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  1749 Jun  8 19:49 daily_pipeline.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  5814 Jun  8 18:37 events_master_pipeline.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun   831 Jun  8 03:59 update_live_inference_stream.py.    
-rw-r--r--  1 jupyter-arun jupyter-arun   968 Jun  8 03:09 update_pm_curated_constraints.py.    
-rw-r--r--  1 jupyter-arun jupyter-arun  2414 Jun  8 01:01 ml_feature_store_historical_backfill.py.   
drwxr-xr-x 10 root         root          4096 Jun  5 20:41 ..     
-rw-r--r--  1 jupyter-arun jupyter-arun  5862 May 29 21:25 feature_engineering.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  5118 May 28 00:46 pm_batch_dq.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  3299 May 20 19:57 pm_kafka_to_iceberg.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  7717 May 20 19:13 generate_random_pm_data.py.   
drwxrwxr-x  2 jupyter-arun jupyter-arun  4096 May 19 22:54 .cde.   
-rw-------  1 jupyter-arun jupyter-arun  9385 May 19 22:54 .viminfo.   
-rw-r--r--  1 jupyter-arun jupyter-arun   689 May 16 23:35 load_neighbors.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  1209 May 16 23:20 load_events.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun   656 May 16 21:23 load_topology.py.   
-rw-r--r--  1 jupyter-arun jupyter-arun  1069 May 16 19:40 cleanup_data.py.   
-rw-rw-r--  1 jupyter-arun jupyter-arun    21 May 12 00:30 requirements.txt.   
drwx------  3 jupyter-arun jupyter-arun  4096 May 11 20:34 .cache.  
drwxr-xr-x  3 jupyter-arun jupyter-arun  4096 May 11 03:10 .ipython.   
drwxr-xr-x  3 jupyter-arun jupyter-arun  4096 Apr  7 04:46 .jupyter.   
drwxr-xr-x  3 jupyter-arun jupyter-arun  4096 Apr  7 01:33 .local.  
-rw-r--r--  1 jupyter-arun jupyter-arun   220 Mar 31  2024 .bash_logout.    
-rw-r--r--  1 jupyter-arun jupyter-arun  3771 Mar 31  2024 .bashrc.    
-rw-r--r--  1 jupyter-arun jupyter-arun   807 Mar 31  2024 .profile.     