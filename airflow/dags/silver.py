# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator

# # ================== DAG CONFIG ==================
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=3),
# }

# with DAG(
#     dag_id='silver_pipeline_dag',
#     default_args=default_args,
#     description='Chạy pipeline Silver qua docker exec Spark',
#     schedule_interval='0 8 * * *',  # hoặc đặt cron '0 * * * *' nếu muốn chạy định kỳ
#     start_date=datetime(2025, 10, 10),
#     catchup=False,
#     tags=['lakehouse', 'spark', 'silver'],
# ) as dag:

#     # ================== TASK: Run Spark Silver Script ==================
#     run_silver = BashOperator(
#         task_id='run_silver_pipeline',
#         bash_command=(
#             "docker exec spark-master "
#             "/opt/bitnami/spark/bin/spark-submit "
#             "--master spark://spark-master:7077 "
#             "/opt/bitnami/spark/scripts/silver.py"
#         )
#     )

#     run_silver

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ================== DAG CONFIG ==================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='silver_pipeline_dag',
    default_args=default_args,
    description='Chạy pipeline Silver (PySpark + Delta Lake) qua docker exec Spark',
    schedule_interval='0 8 * * *',  # Chạy mỗi ngày lúc 08:00 sáng
    start_date=datetime(2025, 10, 10),
    catchup=False,
    tags=['lakehouse', 'spark', 'silver'],
) as dag:

    # ================== TASK: Run Silver Script ==================
    run_silver = BashOperator(
        task_id='run_silver_pipeline',
        bash_command=(
            "echo '=== 🚀 Bắt đầu chạy Silver Pipeline ===' && "
            "docker exec spark-master /opt/bitnami/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.19 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/bitnami/spark/scripts/silver.py && "
            "echo '✅ Hoàn tất Silver Pipeline'"
        )
    )

    run_silver
