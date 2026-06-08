import os
import math
import xgboost as xgb
import pandas as pd
import traceback
from datetime import datetime
from impala.dbapi import connect
from impala.util import as_pandas
import cml.models_v1 as models

print("Booting up Ohana Wireless Traffic Predictor API (Production Release v3)...")

ohana_wireless_model = None
impala_conn = None
startup_error = ""

# ==============================================================================
# 1. INITIALIZATION PHASE (Runs once when the API container starts)
# ==============================================================================
try:
    # A. Load the Champion XGBoost Model
    model_path = "/home/cdsw/models/ohana_wireless_model.json"
    print(f"Loading native XGBoost model from {model_path}...")
    ohana_wireless_model = xgb.XGBRegressor()
    ohana_wireless_model.load_model(model_path)
    
    # B. Establish a persistent, high-performance connection to Impala
    IMPALA_HOST = os.getenv("IMPALA_HOST", 'coordinator-ps-amer-impala-warehouse.dw-ps-amer-sandbox-aws.a465-9q4k.cloudera.site')
    WORKLOAD_USER = os.getenv("WORKLOAD_USER", "athangamani")
    WORKLOAD_PASSWORD = os.getenv("WORKLOAD_PASSWORD", "BlancaLake123")
    
    impala_conn = connect(
        host=IMPALA_HOST, port=443, use_ssl=True, auth_mechanism='LDAP',
        user=WORKLOAD_USER, password=WORKLOAD_PASSWORD, use_http_transport=True, http_path='cliservice'
    )
    print("✅ Ohana Wireless model and Impala Data Lake connection loaded successfully.")

except Exception as e:
    startup_error = traceback.format_exc()
    print("CRITICAL ERROR DURING STARTUP:\n", startup_error)


# ==============================================================================
# 2. INFERENCE PHASE (Runs every time an API request is received)
# ==============================================================================
@models.cml_model
def predict(args):
    # Failsafe: Ensure the container started correctly
    if ohana_wireless_model is None or impala_conn is None:
        return {"status": "fatal_startup_error", "traceback": startup_error}
        
    # Extract the target tower from the JSON payload
    cell_id = args.get("cell_id")
    if not cell_id:
        return {"status": "error", "message": "Missing required parameter: 'cell_id'"}

    try:
        cursor = impala_conn.cursor()
        
        # A. Query the Live Stream via Impala (Lightning-fast single table scan)
        query = f"""
            SELECT 
                rolling_ul_utilization_pct,
                rolling_dl_throughput_mbps,
                rolling_active_ue_count
            FROM ohana.live_inference_stream 
            WHERE cell_id = '{cell_id}'
            ORDER BY window_start DESC
            LIMIT 1
        """
        cursor.execute(query)
        live_data_df = as_pandas(cursor)
        
        if live_data_df.empty:
            return {"status": "error", "message": f"No live telemetry found for {cell_id} in the stream."}

        # B. Calculate the Time Features dynamically in memory
        now = datetime.now()
        live_data_df['hour_sin'] = math.sin(now.hour * (2 * math.pi / 24))
        live_data_df['hour_cos'] = math.cos(now.hour * (2 * math.pi / 24))
        live_data_df['dow_sin'] = math.sin(now.weekday() * (2 * math.pi / 7))
        live_data_df['dow_cos'] = math.cos(now.weekday() * (2 * math.pi / 7))
        
        # C. Align schema and safely fill missing features (Neighbors & Deep Lags)
        expected_features = ohana_wireless_model.feature_names_in_
        for feature in expected_features:
            if feature not in live_data_df.columns:
                # XGBoost natively understands how to route NaN through its decision trees
                live_data_df[feature] = float('nan')
                
        # Reorder to match the exact sequence the model was trained on
        input_vector = live_data_df[expected_features]
        
        # Enforce strict numeric types to prevent JSON serialization type mismatches
        for col in input_vector.columns:
            input_vector[col] = pd.to_numeric(input_vector[col], errors='coerce')
            
        # D. Execute the Prediction
        prediction = ohana_wireless_model.predict(input_vector)
        predicted_value = float(prediction[0])
        
        # E. Determine Alert Level for downstream Agent routing
        if predicted_value > 90.0:
            alert_level = "CRITICAL"
        elif predicted_value > 80.0:
            alert_level = "WARNING"
        else:
            alert_level = "NORMAL"
            
        return {
            "status": "success",
            "target_cell": cell_id,
            "timestamp": now.isoformat(),
            "predicted_prb_utilization_pct": predicted_value,
            "alert_level": alert_level
        }
        
    except Exception as e:
        return {
            "status": "prediction_error",
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }