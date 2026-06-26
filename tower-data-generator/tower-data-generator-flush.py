import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import paramiko
import io

# --- AWS SFTP CONFIGURATION ---
SFTP_HOST = "35.91.66.114"
SFTP_USER = "ubuntu"
SFTP_KEY_PATH = "athangamani-cdp-oregon.pem"
UPLOAD_DIR = "/home/ubuntu/cell_data_drop/"

TARGET_ALERT_CELL = "CELL001801"
CELL_TOWERS = [TARGET_ALERT_CELL, "CELL001802", "CELL001803", "CELL001804", "CELL001805"]

BATCHES_TO_GENERATE = 8   
SLEEP_BETWEEN_BATCHES = 5 

def generate_telemetry(cell_id, simulated_time):
    if cell_id == TARGET_ALERT_CELL:
        # 💥 CHAOS MONKEY
        ul_util = round(np.random.uniform(96.0, 99.2), 2)
        dl_thru = round(np.random.uniform(3.5, 9.8), 2)       
        active_ue = int(np.random.uniform(42000, 48000))      
        rrc_conn = int(active_ue * 0.97)
    else:
        # Normal
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
    print(f"Initializing Sequential CHAOS Flush for {TARGET_ALERT_CELL}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(SFTP_HOST, username=SFTP_USER, key_filename=SFTP_KEY_PATH)
        sftp = ssh.open_sftp()
        
        # Baseline time starts NOW
        current_sim_time = datetime.utcnow()
        print(f"Anomaly Start Time (Simulated): {current_sim_time.strftime('%H:%M:%S')}")
        
        for batch_num in range(BATCHES_TO_GENERATE):
            print(f"[{current_sim_time.strftime('%H:%M:%S')}] Pushing Minute {batch_num + 1}/{BATCHES_TO_GENERATE}")
            
            for cell in CELL_TOWERS:
                df = pd.DataFrame([generate_telemetry(cell, current_sim_time)])
                timestamp_str = current_sim_time.strftime('%Y%m%d_%H%M%S')
                filename = f"PM_{cell}_{timestamp_str}.csv"
                
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                
                bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
                remote_path = f"{UPLOAD_DIR}{filename}"
                sftp.putfo(bytes_buffer, remote_path, confirm=False)
                
            # Increment the clock
            current_sim_time += timedelta(minutes=5)
            time.sleep(SLEEP_BETWEEN_BATCHES)

        sftp.close()
        ssh.close()
        print("\n💥 Sequential Flush Complete! 40 simulated minutes of anomaly data delivered.")
        
    except Exception as e:
        print(f"Failed to connect or upload: {e}")

if __name__ == "__main__":
    push_to_ftp_sequential()