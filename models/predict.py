# predict.py
import xgboost as xgb
import pandas as pd
import traceback
import cml.models_v1 as models

print("Booting up Ohana Wireless Traffic Predictor API (Production Release)...")

ohana_wireless_model = None
startup_error = ""

try:
    model_path = "/home/cdsw/models/ohana_wireless_model.json"
    print(f"Loading native XGBoost model from {model_path}...")
    ohana_wireless_model = xgb.XGBRegressor()
    ohana_wireless_model.load_model(model_path)
    print("✅ Ohana Wireless model loaded successfully. Ready for requests.")
except Exception as e:
    startup_error = traceback.format_exc()
    print("CRITICAL ERROR DURING STARTUP:")
    print(startup_error)

@models.cml_model
def predict(args):
    if ohana_wireless_model is None:
        return {
            "status": "fatal_startup_error",
            "message": "The Ohana Wireless JSON model failed to load.",
            "traceback": startup_error
        }
        
    try:
        input_df = pd.DataFrame([args])
        expected_features = ohana_wireless_model.feature_names_in_
        
        # --- THE FIX: Gracefully handle missing columns in the JSON payload ---
        for feature in expected_features:
            if feature not in input_df.columns:
                # XGBoost natively understands how to route missing data (NaN) through its trees
                input_df[feature] = float('nan')
                
        # Reorder to match training exactly
        input_df = input_df[expected_features]
        
        # Enforce numeric types just in case the JSON sent strings instead of ints/floats
        for col in input_df.columns:
            input_df[col] = pd.to_numeric(input_df[col], errors='coerce')
            
        prediction = ohana_wireless_model.predict(input_df)
        
        return {
            "status": "success",
            "predicted_prb_utilization_pct": float(prediction[0]),
            "deployment_type": "native_json"
        }
        
    except Exception as e:
        return {
            "status": "prediction_error",
            "error_message": str(e)
        }