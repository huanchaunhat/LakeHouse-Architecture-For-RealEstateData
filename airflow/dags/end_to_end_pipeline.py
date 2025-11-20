import logging
import requests
import time
import json
from datetime import datetime, timedelta
from io import BytesIO

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Import S3Hook

# ==================== CONFIGS ====================
# Config cho dbt
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Config cho crawl
BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 20
LIMIT = 20

# Config cho MinIO (Dùng S3Hook)
MINIO_CONN_ID = "minio_s3"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
BRONZE_JSON_PATH = f"{BRONZE_PREFIX}json/" # Thư mục con cho file jsonl
SEEN_FILE = f"{BRONZE_PREFIX}list_ids.txt"

# ==================== HELPER FUNCTIONS ====================

def fetch_list(offset=0, limit=20):
    """Lấy danh sách quảng cáo từ Chợ Tốt"""
    try:
        params = {"cg": 1000, "o": offset, "limit": limit}
        r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("ads", [])
    except Exception as e:
        logging.error(f"Lỗi khi fetch list (offset={offset}): {e}")
        return []

def fetch_detail(list_id, retries=3):
    """Lấy chi tiết từng quảng cáo, có retry"""
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
            logging.error(f"Lỗi {list_id} (lần {attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)
    return None

def load_seen_ids_from_s3(s3_hook: S3Hook):
    """Đọc file seen_ids bằng S3Hook"""
    try:
        file_content = s3_hook.read_key(SEEN_FILE, BUCKET)
        lines = file_content.splitlines()
        return set(line.strip() for line in lines if line.strip())
    except Exception as e:
        logging.warning(f"Chưa có {SEEN_FILE} hoặc lỗi: {e}. Tạo set mới.")
        return set()

def save_seen_ids_to_s3(s3_hook: S3Hook, new_ids: set, existing_ids: set):
    """Lưu file seen_ids bằng S3Hook"""
    all_ids = existing_ids.union(new_ids)
    data_str = "\n".join(all_ids)
    s3_hook.load_string(
        string_data=data_str,
        key=SEEN_FILE,
        bucket_name=BUCKET,
        replace=True
    )
    logging.info(f"Cập nhật {len(new_ids)} ID mới vào {SEEN_FILE}")


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
        Task 1: Crawl API và lưu file .jsonl vào MinIO
        (Đây là logic từ bronze.py đã được refactor)
        """
        logging.info("Bắt đầu task crawl...")
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        seen_ids = load_seen_ids_from_s3(s3_hook)
        new_ids = set()
        all_details = []

        for p in range(1, MAX_PAGES + 1):
            offset = (p - 1) * LIMIT
            ads = fetch_list(offset, LIMIT)
            if not ads:
                break

            for ad in ads:
                list_id = str(ad.get("list_id"))
                if not list_id or list_id in seen_ids:
                    continue

                detail = fetch_detail(list_id)
                if detail:
                    all_details.append(detail)
                    new_ids.add(list_id) # Dùng set.add() tốt hơn

            logging.info(f"Trang {p}: thu được {len(all_details)} bản ghi mới")
            time.sleep(0.2)

        if not all_details:
            logging.info("Không có dữ liệu mới để lưu.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Ghi vào thư mục con /json/ và dùng đuôi .jsonl
        key = f"{BRONZE_JSON_PATH}crawl_{timestamp}.jsonl" 
        
        try:
            # Tạo data_str định dạng JSON Lines
            data_str = "\n".join(json.dumps(detail, ensure_ascii=False) for detail in all_details)
            
            # Dùng S3Hook để upload
            s3_hook.load_string(
                string_data=data_str,
                key=key,
                bucket_name=BUCKET
            )
            logging.info(f"Đã lưu {len(all_details)} bản ghi mới vào {key}")
            
            # Cập nhật file seen_ids
            save_seen_ids_to_s3(s3_hook, new_ids, seen_ids)
            
        except Exception as e:
            logging.error(f"Lỗi trong quá trình lưu dữ liệu: {e}")
            raise # Ném lỗi để Airflow biết task này failed


    # Task 2: Chạy Spark để load .jsonl -> Bảng Delta thô
    load_bronze_to_table = SparkSubmitOperator(
        task_id='load_bronze_to_table',
        application='/usr/local/airflow/scripts/load_bronze_to_table.py',
        conn_id='spark_default', # Cấu hình trong Airflow UI
        packages='io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf={
            # --- 1. KẾT NỐI ---
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.sql.warehouse.dir": "s3a://lakehouse/warehouse",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
            
            # --- 2. MẠNG ---
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "30000",
            "spark.blockManager.port": "30001",

            # --- 3. TÀI NGUYÊN ---

            "spark.dynamicAllocation.enabled": "false",
            "spark.cores.max": "2",
            "spark.executor.memory": "512m",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "16"
        }
    )

    wait_for_thrift_server = BashOperator(
        task_id='wait_for_thrift_server',
        bash_command="""
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
            log.info(f'{host}:{port} is available! Waiting extra 30s for full initialization...')
            time.sleep(30)
            exit(0)
    except (socket.timeout, ConnectionRefusedError):
        log.info(f'Service not yet available ({host}:{port}), retrying in 5 seconds...')
        time.sleep(5)
"
"""
    )

    # Task 3: Chạy dbt để biến đổi
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        retries=3,
        retry_delay=timedelta(seconds=30)
    )

    # Task 4: Chạy dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    # Định nghĩa thứ tự chạy
    run_bronze_crawl() >> load_bronze_to_table >> wait_for_thrift_server >> dbt_run >> dbt_test

# Khởi tạo DAG
elt_pipeline()