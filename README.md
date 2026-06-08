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


Now setting up the nifi flow to read ftp data from the ubuntu box
ssh ubuntu@35.91.66.114
Password Ohana2026!
this is not working yet
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

