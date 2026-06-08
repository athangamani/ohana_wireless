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

# --- SIMULATION CONFIGURATION ---
CELL_TOWERS = ["SEA-001", "SEA-002", "ISQ-001", "BLL-001"] 

# FLUSH CONFIGURATION
FAST_FORWARD_MINUTES = 30 # Generate 30 minutes of data
BATCHES_TO_GENERATE = 6   # 6 batches * 5 min = 30 minutes
SLEEP_BETWEEN_BATCHES = 1 # 1 second sleep just to give NiFi breathing room

def generate_telemetry(cell_id, simulated_time):
    """Generates a single row of synthetic tower telemetry with a FORCED timestamp"""
    return {
        "cell_id": cell_id,
        "collection_timestamp": simulated_time.isoformat(),
        "technology": "5G_NR",
        "ul_prb_utilization_pct": round(np.random.uniform(10.0, 85.0), 2),
        "dl_throughput_mbps": round(np.random.uniform(50.0, 800.0), 2),
        "active_ue_count": int(np.random.uniform(50, 1200)),
        "rrc_connected_avg": int(np.random.uniform(40, 1100)),
        "availability_pct": 100.0
    }

def push_to_ftp_fast_forward():
    print(f"Initializing Fast-Forward Flush Sequence...")
    
    # Connect to AWS SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(SFTP_HOST, username=SFTP_USER, key_filename=SFTP_KEY_PATH)
        sftp = ssh.open_sftp()
        
        # Start the clock right now
        current_sim_time = datetime.now()
        
        for batch_num in range(BATCHES_TO_GENERATE):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Pushing Batch {batch_num + 1}/{BATCHES_TO_GENERATE} (Simulated Time: {current_sim_time.strftime('%H:%M')})")
            
            for cell in CELL_TOWERS:
                # Pass the manipulated time to the generator
                df = pd.DataFrame([generate_telemetry(cell, current_sim_time)])
                
                # Create standard Telco filename using the SIMULATED time
                timestamp_str = current_sim_time.strftime('%Y%m%d_%H%M%S')
                filename = f"PM_{cell}_{timestamp_str}.csv"
                
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                
                bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
                remote_path = f"{UPLOAD_DIR}{filename}"
                sftp.putfo(bytes_buffer, remote_path, confirm=False)
                
            # Advance the simulated clock by 5 minutes for the next batch
            current_sim_time += timedelta(minutes=5)
            
            # Sleep 1 second to not overwhelm paramiko/NiFi
            time.sleep(SLEEP_BETWEEN_BATCHES)

        sftp.close()
        ssh.close()
        print("Flush Sequence Complete. Check Iceberg!")
        
    except Exception as e:
        print(f"Failed to connect or upload to AWS: {e}")

if __name__ == "__main__":
    push_to_ftp_fast_forward()