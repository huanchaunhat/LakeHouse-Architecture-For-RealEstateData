import logging
import requests
import time
import json
from datetime import datetime, timedelta
from io import BytesIO

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Import S3Hook

# ==================== CONFIGS ====================
# dbt configuration
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Crawl configuration
BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 20
LIMIT = 20

# MinIO configuration (using S3Hook)
MINIO_CONN_ID = "minio_s3"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
BRONZE_JSON_PATH = f"{BRONZE_PREFIX}json/" # Subdirectory for jsonl files
SEEN_FILE = f"{BRONZE_PREFIX}list_ids.txt"

# ==================== HELPER FUNCTIONS ====================

def fetch_list(offset=0, limit=20):
    """Fetch advertisement list from Chotot"""
    try:
        params = {"cg": 1000, "o": offset, "limit": limit}
        r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("ads", [])
    except Exception as e:
        logging.error(f"Error fetching list (offset={offset}): {e}")
        return []

def fetch_detail(list_id, retries=3):
    """Fetch advertisement details with retry"""
    for attempt in range(retries):
        try:
            r = requests.get(DETAIL_URL.format(list_id), headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            ad = data.get("ad", {})
            result = {
                "list_id": list_id,
                "title": ad.get("subject"),
                "price": ad.get("price_string"),
                "address": ad.get("address") or None,
                "images": ad.get("images", []),
            }
            for p in data.get("parameters", []):
                if "label" in p and "value" in p:
                    result[p["label"]] = p["value"]
            return result
        except Exception as e:
            logging.error(f"Error {list_id} (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)
    return None

def load_seen_ids_from_s3(s3_hook: S3Hook):
    """Read seen_ids file using S3Hook"""
    try:
        file_content = s3_hook.read_key(SEEN_FILE, BUCKET)
        lines = file_content.splitlines()
        return set(line.strip() for line in lines if line.strip())
    except Exception as e:
        logging.warning(f"File not found {SEEN_FILE} or error: {e}. Creating new set.")
        return set()

def save_seen_ids_to_s3(s3_hook: S3Hook, new_ids: set, existing_ids: set):
    """Save seen_ids file using S3Hook"""
    all_ids = existing_ids.union(new_ids)
    data_str = "\n".join(all_ids)
    s3_hook.load_string(
        string_data=data_str,
        key=SEEN_FILE,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Updated {len(new_ids)} new IDs to {SEEN_FILE}")


# ==================== AIRFLOW DAG ====================

@dag(
    dag_id='end_to_end_lakehouse_pipeline',
    default_args={'owner': 'airflow', 'retries': 1},
    description='Pipeline ELT: Crawl (Python) -> Load (Spark) -> Transform (dbt)',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 10),
    catchup=False,
    tags=['lakehouse', 'elt', 'end-to-end'],
)
def elt_pipeline():

    @task
    def run_bronze_crawl():
        """
        Task 1: Crawl API and save file .jsonl to MinIO
        FIX: Crawl all records (not skip seen_ids) to capture price updates
        Bronze layer will handle deduplication
        """
        logging.info("Starting crawl task...")
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        # seen_ids = load_seen_ids_from_s3(s3_hook)
        new_ids = set()
        all_details = []

        for p in range(1, MAX_PAGES + 1):
            offset = (p - 1) * LIMIT
            ads = fetch_list(offset, LIMIT)
            if not ads:
                break

            for ad in ads:
                list_id = str(ad.get("list_id"))
                if not list_id:
                    continue

                detail = fetch_detail(list_id)
                if detail:
                    all_details.append(detail)
                    new_ids.add(list_id)

            logging.info(f"Page {p}: fetched {len(all_details)} records (including updates)")
            time.sleep(0.2)

        if not all_details:
            logging.info("No new data to save.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Write to /json/ subdirectory with .jsonl extension
        key = f"{BRONZE_JSON_PATH}crawl_{timestamp}.jsonl" 
        
        try:
            # Create data_str in JSON Lines format
            data_str = "\n".join(json.dumps(detail, ensure_ascii=False) for detail in all_details)
            
            # Use S3Hook to upload
            s3_hook.load_string(
                string_data=data_str,
                key=key,
                bucket_name=BUCKET
            )
            logging.info(f"Saved {len(all_details)} records (including updates) to {key}")
            
            # Update seen_ids file (load existing + merge with new)
            existing_ids = load_seen_ids_from_s3(s3_hook)
            save_seen_ids_to_s3(s3_hook, new_ids, existing_ids)
            
        except Exception as e:
            logging.error(f"Error during data save process: {e}")
            raise # Raise error so Airflow knows this task failed


    # Task 2: Run Spark to load .jsonl -> Delta table
    load_bronze_to_table = BashOperator(
        task_id='load_bronze_to_table',
        bash_command="""
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.sql.warehouse.dir=s3a://lakehouse/warehouse \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/bitnami/spark/scripts/load_bronze_to_table.py
        """
    )

    # Task 2.5: Normalize column names (Vietnamese to English)
    normalize_bronze_columns = BashOperator(
        task_id='normalize_bronze_columns',
        bash_command="""
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.sql.warehouse.dir=s3a://lakehouse/warehouse \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
    /opt/bitnami/spark/scripts/normalize_bronze_columns.py
        """
    )

    # Task 2.7: Restart and wait for Thrift Server (ensure clean state)
    restart_and_wait_thrift = BashOperator(
        task_id='restart_and_wait_thrift',
        bash_command="""
echo "Restarting Spark Thrift Server for clean state..."
docker restart spark-thrift-server
sleep 10

python3 -c "
import socket
import time
import logging

host = 'spark-thrift-server'
port = 10000
timeout_seconds = 120
start_time = time.time()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

log.info(f'Waiting for {host}:{port} to be available...')
while True:
    if time.time() - start_time > timeout_seconds:
        log.error('Timeout exceeded. Service is not available.')
        exit(1)
    try:
        with socket.create_connection((host, port), timeout=5):
            log.info(f'{host}:{port} is available! Waiting extra 20s for full initialization...')
            time.sleep(20)
            exit(0)
    except (socket.timeout, ConnectionRefusedError):
        log.info(f'Service not yet available ({host}:{port}), retrying in 5 seconds...')
        time.sleep(5)
"
"""
    )

    # Task 3: Run dbt for transformation with auto full-refresh if tables don't exist
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
# Check if silver.stg_properties exists, if not do full-refresh
if docker exec spark-master spark-sql \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    -e "DESCRIBE TABLE silver.stg_properties" 2>&1 | grep -q "Table or view not found"; then
    echo "Tables don't exist, running dbt with --full-refresh"
    dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && \
    dbt run --full-refresh --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
else
    echo "Tables exist, running dbt incremental"
    dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && \
    dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}
fi
        """,
        retries=3,
        retry_delay=timedelta(seconds=30)
    )

    # Task 4: Run dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    # Task 5: Stop Spark Thrift Server to free resources
    stop_thrift_server = BashOperator(
        task_id='stop_thrift_server',
        bash_command="""
echo "Stopping Spark Thrift Server to free CPU resources..."
docker exec spark-thrift-server pkill -f "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2" || true
echo "Thrift Server stopped successfully"
        """,
        trigger_rule='all_done'  # Run even if previous tasks fail
    )

    run_bronze_crawl() >> load_bronze_to_table >> normalize_bronze_columns >> restart_and_wait_thrift >> dbt_run >> dbt_test >> stop_thrift_server
  
# Initialize DAG
elt_pipeline()