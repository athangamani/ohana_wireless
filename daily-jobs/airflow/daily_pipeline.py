from datetime import datetime, timedelta
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

default_args = {
    'owner': 'ohana_data_engineering',
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 25),
}

# The DAG: Runs daily at 2:00 AM
with DAG(
    'ohana_daily_feature_engineering',
    default_args=default_args,
    schedule_interval='0 2 * * *', 
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    # Trigger the PySpark job we will create in CDE, passing the Airflow execution date
    run_incremental_features = CDEJobRunOperator(
        task_id='run_feature_engineering_incremental',
        job_name='pm-feature-engineering-incremental',
        # THE FIX: Use overrides to pass strict CLI arguments to the PySpark script
        overrides={
            "spark": {
                "args": ["--execution_date", "{{ ds }}"]
            }
        }
    )

    run_incremental_features