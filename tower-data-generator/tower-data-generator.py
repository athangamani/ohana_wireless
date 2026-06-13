import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, timezone
import paramiko
import io

# --- AWS SFTP CONFIGURATION ---
SFTP_HOST = "35.91.66.114"
SFTP_USER = "ubuntu"
SFTP_KEY_PATH = "athangamani-cdp-oregon.pem"
UPLOAD_DIR = "/home/ubuntu/cell_data_drop/"

# --- SIMULATION CONFIGURATION ---
# Using some Pacific Northwest towers for the simulation
CELL_TOWERS = ["CELL001801", "CELL001802", "CELL001803", "CELL001804", "CELL001805"]
INTERVAL_SECONDS = 300 # 5 minutes

def generate_telemetry(cell_id):
    """Generates a single row of synthetic tower telemetry"""
    return {
        "cell_id": cell_id,
        "collection_timestamp": datetime.utcnow().isoformat(),
        "technology": "5G_NR",
        "ul_prb_utilization_pct": round(np.random.uniform(10.0, 85.0), 2),
        "dl_throughput_mbps": round(np.random.uniform(50.0, 800.0), 2),
        "active_ue_count": int(np.random.uniform(50, 1200)),
        "rrc_connected_avg": int(np.random.uniform(40, 1100)),
        "availability_pct": 100.0
    }

def push_to_ftp():
    current_sim_time = datetime.utcnow()
    print(f"[{current_sim_time.strftime('%H:%M:%S')}] Waking up to generate PM files...")
    
    # Connect to AWS SFTP
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(SFTP_HOST, username=SFTP_USER, key_filename=SFTP_KEY_PATH)
        sftp = ssh.open_sftp()
        
        for cell in CELL_TOWERS:
            # Generate Data
            df = pd.DataFrame([generate_telemetry(cell)])
            
            # Create standard Telco filename
            timestamp_str = current_sim_time.strftime('%Y%m%d_%H%M%S')
            filename = f"PM_{cell}_{timestamp_str}.csv"
            
            # Write DataFrame to a string buffer
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Reset the buffer's position to the beginning before uploading
            csv_buffer.seek(0)
            
            # --- THE FIX: Convert StringIO to BytesIO for paramiko ---
            # Paramiko requires byte streams, not string streams, for direct buffer uploads
            bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
            
            remote_path = f"{UPLOAD_DIR}{filename}"
            
            # Upload using putfo (Put File Object) and skip the stat confirmation
            sftp.putfo(bytes_buffer, remote_path, confirm=False)
                
            print(f"  -> Uploaded {filename}")
            
        sftp.close()
        ssh.close()
        print("Sleep sequence initiated. See you in 5 minutes.")
        
    except Exception as e:
        print(f"Failed to connect or upload to AWS: {e}")

# Run the infinite loop
if __name__ == "__main__":
    print("Initializing Cell Tower Edge Simulator...")
    while True:
        push_to_ftp()
        time.sleep(INTERVAL_SECONDS)