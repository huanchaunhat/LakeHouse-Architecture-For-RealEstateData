#!/usr/bin/env python3
"""
Silver Pipeline - Transform từ Bronze và lưu vào MinIO (Silver Layer) - PySpark + Delta Lake
"""

import os
import re
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType, IntegerType
from delta import configure_spark_with_delta_pip

# ==================== CONFIG ====================
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
PROCESSED_PREFIX = "bronze/processed/"
SILVER_PREFIX = "silver/"
PROCESS_ALL = True

# ==================== MinIO Client ====================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Auto-create bucket nếu chưa có
existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)
    print(f"🪣 Created bucket: {BUCKET}")

# ==================== HELPERS ====================
def parse_area(value):
    if not value:
        return None
    try:
        nums = re.findall(r'[\d,.]+', str(value))
        return float(nums[0].replace(",", "")) if nums else None
    except:
        return None


def parse_number(value):
    if not value:
        return None
    try:
        return int(float(str(value)))
    except:
        return None


def normalize_price(value):
    if not value:
        return None
    s = str(value).lower()
    try:
        if "tỷ" in s:
            nums = re.findall(r'[\d.]+', s)
            return float(nums[0]) if nums else None
        elif "triệu" in s:
            nums = re.findall(r'[\d.]+', s)
            return float(nums[0]) / 1000 if nums else None
        else:
            price_str = re.sub(r"[^\d]", "", str(value))
            return int(price_str) / 1e9 if price_str else None
    except:
        return None


def clean_column_names(df):
    """Chuẩn hóa tên cột cho hợp lệ với Delta Lake"""
    for old_name in df.columns:
        new_name = (
            old_name.strip()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(";", "")
            .replace(",", "")
            .replace("=", "")
            .replace("/", "_")
        )
        df = df.withColumnRenamed(old_name, new_name)
    return df


# Register UDFs
from pyspark.sql.functions import udf
parse_area_udf = udf(parse_area, DoubleType())
parse_number_udf = udf(parse_number, IntegerType())
normalize_price_udf = udf(normalize_price, DoubleType())

# ==================== SPARK SESSION ====================
builder = (
    SparkSession.builder
    .appName("SilverPipeline")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

# ==================== MAIN ====================
def run_silver():
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Spark session started with Delta Lake")

    # Lấy danh sách file trong Bronze
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX)
    objs = resp.get("Contents", []) if resp else []
    objs = [
        o for o in objs
        if not o["Key"].startswith(PROCESSED_PREFIX) and o["Key"].endswith(".json")
    ]

    if not objs:
        print("⚠️ Không tìm thấy dữ liệu Bronze chưa xử lý.")
        return

    to_process = sorted(objs, key=lambda x: x["LastModified"])
    if not PROCESS_ALL:
        to_process = [to_process[-1]]

    for obj in to_process:
        key = obj["Key"]
        print(f"🔄 Xử lý file: {key}")

        # Đọc JSON từ MinIO
        raw_bytes = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(data, dict):
                data = [data]
        except Exception as e:
            print(f"❌ Không parse được JSON từ {key}: {e}")
            move_to_processed(key)
            continue

        if not data:
            print(f"⚠️ File rỗng: {key}")
            move_to_processed(key)
            continue

        # Load vào Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in data]))
        print(f"📦 Số record raw: {df.count()}")

        # Chuẩn hóa schema
        df_standard = df.select(
            col("address").alias("Address"),
            col("Diện tích đất").alias("Area"),
            col("Chiều ngang").alias("Frontage"),
            col("Đặc điểm nhà/đất").alias("Access_Road"),
            col("Hướng cửa chính").alias("House_Direction"),
            col("Tổng số tầng").alias("Floors"),
            col("Số phòng ngủ").alias("Bedrooms"),
            col("Số phòng vệ sinh").alias("Bathrooms"),
            col("Giấy tờ pháp lý").alias("Legal_Status"),
            col("Tình trạng nội thất").alias("Furniture_State"),
            col("price").alias("Price")
        )

        df_standard = clean_column_names(df_standard)
        print(f"✅ Sau transform: {df_standard.count()} records")

        # Lấy timestamp từ tên file bronze
        filename = os.path.basename(key)
        timestamp = filename.split("_")[1]  # crawl_YYYYMMDD_HHMMSS.json
        date_fmt = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:]}"  # YYYY-MM-DD

        # Tạo key dạng partitioned
        silver_key = (
            key.replace(BRONZE_PREFIX, f"{SILVER_PREFIX}/date={date_fmt}/")
            .replace(".json", "")
            .replace("crawl_", "crawl_cleaned_")
        )
        silver_path = f"s3a://{BUCKET}/{silver_key}"

        # Thêm cột Date
        df_standard = df_standard.withColumn("Date", lit(date_fmt))

        # Ghi Delta Table vào MinIO
        df_standard.write.format("delta").mode("overwrite").partitionBy("Date").save(silver_path)
        print(f"💾 Đã lưu Silver (Delta Lake) -> {silver_path}")

        # Move file đã xử lý
        move_to_processed(key)


def move_to_processed(key):
    """Di chuyển file từ bronze/ sang bronze/processed/"""
    try:
        processed_key = key.replace(BRONZE_PREFIX, PROCESSED_PREFIX)
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=processed_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"📦 Đã move {key} -> {processed_key}")
    except Exception as e:
        print(f"⚠️ Lỗi khi move_to_processed: {e}")


if __name__ == "__main__":
    run_silver()
