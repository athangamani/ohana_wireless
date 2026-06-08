from datetime import datetime, timedelta
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# 1. Define Default Arguments and Retries
default_args = {
    'owner': 'athangamani',
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
    'depends_on_past': False,
    # Set start date to a fixed point in the past
    'start_date': datetime(2026, 6, 6),
}

# 2. Instantiate the DAG (Runs every night at 2:00 AM)
dag = DAG(
    'ohana_nightly_ml_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *', 
    catchup=False, # Prevents Airflow from running 100 historical jobs if turned off for a while
    max_active_runs=1,
    tags=['ohana', 'telecom', 'mlops']
)

# 3. Define the Tasks (Pointing to your pre-configured CDE Spark Jobs)

# Task 1: Ingest the latest external events calendar
ingest_events = CDEJobRunOperator(
    task_id='events_master_pipeline',
    job_name='events_master_pipeline_job',
    dag=dag
)

# Task 2: Build the ML Features
incremental_feature_engineering = CDEJobRunOperator(
    task_id='feature_engineering_incremental',
    job_name='feature_engineering_incremental_job',
    variables={'execution_date': '{{ ds }}'},
    dag=dag
)

# Task 3: Calculate Executive Dashboard KPIs
sliding_window_kpis = CDEJobRunOperator(
    task_id='kpi_sliding_window',
    job_name='kpi_sliding_window_job',
    variables={'execution_date': '{{ ds }}'},
    dag=dag
)

# 4. Define the Execution Order (The Dependencies)
# Events MUST finish before Feature Engineering starts. 
# KPIs can run in parallel with Feature Engineering since they both just read from pm_curated.

ingest_events >> incremental_feature_engineering
ingest_events >> sliding_window_kpis