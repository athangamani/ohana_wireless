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


for running xgboost i had to open up a session in CAI and go to terminal access and run the following one by one
pip install scikit-learn.  
pip install xgboost optuna optuna-integration[mlflow] scikit-learn pyiceberg[hive,s3fs] matplotlib.  

