#!/usr/bin/env python3
"""
Silver Pipeline - Transform & Clean từ Bronze và lưu vào MinIO (Silver Layer) - PySpark + Delta Lake
"""

import os
import re
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lit, trim, initcap, when, concat_ws, round as spark_round
)
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

if BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket=BUCKET)
    print(f"🪣 Created bucket: {BUCKET}")

# ==================== HELPERS ====================
def parse_area(v):
    if not v:
        return None
    try:
        nums = re.findall(r'[\d,.]+', str(v))
        return float(nums[0].replace(",", "")) if nums else None
    except:
        return None

def parse_number(v):
    if not v:
        return None
    try:
        return int(float(str(v)))
    except:
        return None

def normalize_price(v):
    if not v:
        return None
    s = str(v).lower()
    try:
        if "tỷ" in s:
            nums = re.findall(r'[\d,.]+', s)
            return float(nums[0].replace(",", ".")) if nums else None
        elif "triệu" in s:
            nums = re.findall(r'[\d,.]+', s)
            return float(nums[0].replace(",", ".")) / 1000 if nums else None
        else:
            nums = re.findall(r'[\d.]+', s)
            return float(nums[0]) if nums else None
    except:
        return None

def clean_column_names(df):
    for old in df.columns:
        new = (
            old.strip()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(";", "")
            .replace(",", "")
            .replace("=", "")
            .replace("/", "_")
        )
        df = df.withColumnRenamed(old, new)
    return df

# Register UDFs
parse_area_udf = udf(parse_area, DoubleType())
parse_number_udf = udf(parse_number, IntegerType())
normalize_price_udf = udf(normalize_price, DoubleType())

# ==================== SPARK ====================
builder = (
    SparkSession.builder
    .appName("SilverPipeline-Delta")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark session started with Delta Lake")

# ==================== MAIN ====================
def run_silver():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX)
    objs = resp.get("Contents", []) if resp else []
    objs = [o for o in objs if not o["Key"].startswith(PROCESSED_PREFIX) and o["Key"].endswith(".json")]
    if not objs:
        print("⚠️ Không tìm thấy dữ liệu Bronze chưa xử lý.")
        return

    to_process = sorted(objs, key=lambda x: x["LastModified"])
    if not PROCESS_ALL:
        to_process = [to_process[-1]]

    for obj in to_process:
        key = obj["Key"]
        print(f"\n🔄 Đang xử lý file: {key}")

        raw_bytes = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(data, dict):
                data = [data]
        except Exception as e:
            print(f"❌ Parse JSON lỗi: {e}")
            move_to_processed(key)
            continue

        if not data:
            print(f"⚠️ File rỗng: {key}")
            move_to_processed(key)
            continue

        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r) for r in data]))
        print(f"📦 Raw records: {df.count()}")

        df = clean_column_names(df)

        # 🧭 Chuẩn hóa cột Address
        df = df.withColumn(
            "Address",
            when(col("address").isNotNull(), trim(col("address")))
            .otherwise(
                when(col("Địa_chỉ").isNotNull(), trim(col("Địa_chỉ")))
                .otherwise(
                    concat_ws(", ",
                        col("Phường,_thị_xã,_thị_trấn"),
                        col("Quận,_Huyện"),
                        col("Tỉnh,_thành_phố")
                    )
                )
            )
        )

        # === Làm sạch dữ liệu ===
        df_clean = (
            df
            .withColumn("Area", parse_area_udf(col("Diện_tích_đất")))
            .withColumn("Frontage", parse_area_udf(col("Chiều_ngang")))
            .withColumn("Floors", parse_number_udf(col("Tổng_số_tầng")))
            .withColumn("Bedrooms", parse_number_udf(col("Số_phòng_ngủ")))
            .withColumn("Bathrooms", parse_number_udf(col("Số_phòng_vệ_sinh")))
            .withColumn("Price", normalize_price_udf(col("price")))
            .withColumn("Address", initcap(trim(col("Address"))))
            .withColumn("Legal_Status", initcap(trim(col("Giấy_tờ_pháp_lý"))))
            .withColumn("House_Direction", initcap(trim(col("Hướng_cửa_chính"))))
            .filter(col("Area").isNotNull() & (col("Area") > 0))
            .filter(col("Price").isNotNull() & (col("Price") > 0))
            .withColumn("Price_per_m2", spark_round(col("Price") / col("Area"), 3))
        )

        cleaned_count = df_clean.count()
        print(f"✅ Sau khi làm sạch: {cleaned_count} bản ghi hợp lệ")

        # === Ghi ra Silver Layer (Delta Lake) ===
        filename = os.path.basename(key)
        timestamp = filename.split("_")[1] if "_" in filename else "unknown"
        date_fmt = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:]}" if len(timestamp) == 8 else "unknown"
        silver_key = (
            key.replace(BRONZE_PREFIX, f"{SILVER_PREFIX}/date={date_fmt}/")
            .replace(".json", "")
            .replace("crawl_", "crawl_cleaned_")
        )
        silver_path = f"s3a://{BUCKET}/{silver_key}"

        df_clean = df_clean.withColumn("Date", lit(date_fmt))
        df_clean.write.format("delta").mode("overwrite").partitionBy("Date").save(silver_path)
        print(f"💾 Đã lưu Silver (Delta Lake) tại: {silver_path}")

        move_to_processed(key)

def move_to_processed(key):
    try:
        processed_key = key.replace(BRONZE_PREFIX, PROCESSED_PREFIX)
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=processed_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"📦 Đã move {key} -> {processed_key}")
    except Exception as e:
        print(f"⚠️ Lỗi khi move_to_processed: {e}")

if __name__ == "__main__":
    run_silver()


# scripts/load_bronze_to_table.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ==================== SPARK ====================
builder = (
    SparkSession.builder
    .appName("SilverPipeline-Delta")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark session started with Delta Lake")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/*.jsonl"
BRONZE_TABLE_NAME = "bronze.raw_properties" # Tên bảng thô
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/bronze_raw"

def load_bronze_to_table():
    df_raw = spark.readStream.format("json") \
        .option("spark.sql.streaming.schemaInference", "true") \
        .load(BRONZE_PATH)

    query = (
        df_raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(once=True)
        .toTable(BRONZE_TABLE_NAME) # Ghi thẳng ra 1 bảng Delta
    )
    query.awaitTermination()
    print(f"✅ Đã load dữ liệu thô vào bảng {BRONZE_TABLE_NAME}")

if __name__ == "__main__":
    load_bronze_to_table()