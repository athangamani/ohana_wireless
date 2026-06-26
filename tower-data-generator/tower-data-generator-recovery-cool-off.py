import os
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import paramiko
import io

SFTP_HOST = os.getenv("SFTP_HOST", "35.91.66.114") 
SFTP_USER = os.getenv("SFTP_USER", "ubuntu")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH", "athangamani-cdp-oregon.pem")
UPLOAD_DIR = "/home/ubuntu/cell_data_drop/"

TARGET_ALERT_CELL = "CELL001801"
CELL_TOWERS = [TARGET_ALERT_CELL, "CELL001802", "CELL001803", "CELL001804", "CELL001805"]

BATCHES_TO_GENERATE = 8   
SLEEP_BETWEEN_BATCHES = 5

def generate_telemetry(cell_id, simulated_time):
    if cell_id == TARGET_ALERT_CELL:
        # 🟢 AGENTIC HEALING STATE (Plummets to ~10%)
        ul_util = round(np.random.uniform(5.0, 15.0), 2)        
        dl_thru = round(np.random.uniform(150.0, 220.0), 2)       
        active_ue = int(np.random.uniform(28000, 31000))  
        rrc_conn = int(active_ue * 0.97)
    else:
        # Normal Background Noise
        ul_util = round(np.random.uniform(10.0, 65.0), 2)
        dl_thru = round(np.random.uniform(150.0, 600.0), 2)
        active_ue = int(np.random.uniform(50, 1200))
        rrc_conn = int(active_ue * 0.9)

    return {
        "cell_id": cell_id,
        "collection_timestamp": simulated_time.isoformat(),
        "technology": "5G_NR",
        "dl_prb_utilization_pct": 0.0, 
        "ul_prb_utilization_pct": ul_util,
        "dl_throughput_mbps": dl_thru,
        "active_ue_count": active_ue,
        "rrc_connected_avg": rrc_conn,
        "availability_pct": 100.0
    }

def push_to_ftp_sequential():
    if not SFTP_USER or not SFTP_KEY_PATH:
        print("❌ ERROR: Missing SFTP_USER or SFTP_KEY_PATH environment variables.")
        return

    print(f"Initializing 🟢 RECOVERY Flush for {TARGET_ALERT_CELL}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    cumulative_records_processed = 0
    cumulative_bytes_uploaded = 0
    
    try:
        ssh.connect(SFTP_HOST, username=SFTP_USER, key_filename=SFTP_KEY_PATH)
        sftp = ssh.open_sftp()
        
        # -----------------------------------------------------------------------
        # THE FIX: Start the clock 60 minutes in the future!
        # -----------------------------------------------------------------------
        current_sim_time = datetime.utcnow() + timedelta(minutes=60)
        print(f"Recovery Start Time (Simulated): {current_sim_time.strftime('%H:%M:%S')} (Offset to clear Spark Window)")
        
        for batch_num in range(BATCHES_TO_GENERATE):
            print(f"[{current_sim_time.strftime('%H:%M:%S')}] Pushing Recovery Minute {batch_num + 1}/{BATCHES_TO_GENERATE}")
            
            for cell in CELL_TOWERS:
                df = pd.DataFrame([generate_telemetry(cell, current_sim_time)])
                timestamp_str = current_sim_time.strftime('%Y%m%d_%H%M%S')
                filename = f"PM_{cell}_{timestamp_str}.csv"
                
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                
                payload_bytes = csv_buffer.getvalue().encode('utf-8')
                bytes_buffer = io.BytesIO(payload_bytes)
                remote_path = f"{UPLOAD_DIR}{filename}"
                sftp.putfo(bytes_buffer, remote_path, confirm=False)
                
                cumulative_records_processed += len(df)
                cumulative_bytes_uploaded += len(payload_bytes)
                
            print(f"   -> Cumulative Metrics: {cumulative_records_processed} total records | {cumulative_bytes_uploaded} bytes processed")
            
            current_sim_time += timedelta(minutes=5)
            
            if batch_num < BATCHES_TO_GENERATE - 1:
                time.sleep(SLEEP_BETWEEN_BATCHES)

        # =======================================================================
        # NEW CODE INSERTED HERE: The Watermark Chaser
        # =======================================================================
        print("\n[FDE Hack] Pushing a Watermark Chaser 20 minutes into the future to flush the final windows...")
        chaser_time = current_sim_time + timedelta(minutes=20)
        chaser_df = pd.DataFrame([generate_telemetry(TARGET_ALERT_CELL, chaser_time)])
        
        csv_buffer = io.StringIO()
        chaser_df.to_csv(csv_buffer, index=False)
        chaser_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
        
        # Uploading the chaser file
        sftp.putfo(chaser_bytes, f"{UPLOAD_DIR}PM_FLUSH_{chaser_time.strftime('%Y%m%d_%H%M%S')}.csv", confirm=False)
        # =======================================================================
        
        sftp.close()
        ssh.close()
        print("\n🟢 Recovery Flush Complete! Spark watermark advanced. Values should drop instantly in Impala.")
        
    except Exception as e:
        print(f"Failed to connect or upload: {e}")

if __name__ == "__main__":
    push_to_ftp_sequential()