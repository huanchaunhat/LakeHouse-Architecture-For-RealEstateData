from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# =========================================
# DAG chạy file scripts/bronze.py
# =========================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_pipeline",
    default_args=default_args,
    description="Crawl raw data và lưu vào MinIO (Bronze Layer)",
    schedule_interval='0 6 * * *',
    start_date=datetime(2025, 10, 10),
    catchup=False,
    tags=["lakehouse", "bronze"],
) as dag:

    run_bronze = BashOperator(
        task_id="run_bronze_script",
        bash_command="python /usr/local/airflow/scripts/bronze.py ",
    )
    
    run_bronze