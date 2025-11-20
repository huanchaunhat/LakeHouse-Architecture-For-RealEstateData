#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col # <-- Cần import cái này
from delta import configure_spark_with_delta_pip

# ==========================================
# 1. CẤU HÌNH
# ==========================================
BUCKET = "lakehouse"
BRONZE_JSON_PATH = f"s3a://{BUCKET}/bronze/json/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/_checkpoints/bronze_raw_properties"

DATABASE_NAME = "bronze"
TABLE_NAME = "raw_properties"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"
# Đường dẫn lưu file vật lý
TABLE_LOCATION = f"s3a://{BUCKET}/warehouse/{DATABASE_NAME}.db/{TABLE_NAME}"

# ==========================================
# 2. SPARK SESSION
# ==========================================
builder = (
    SparkSession.builder
    .appName("LoadBronzeToJsonTable")
    .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
    .enableHiveSupport()
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. LOGIC CHÍNH
# ==========================================
def run_load_to_bronze_table():
    print(f"Bắt đầu xử lý bảng: {FULL_TABLE_NAME}")

    # 1. Tạo Database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    # 2. Suy luận Schema (Infer Schema)
    print(f"Đang suy luận schema từ: {BRONZE_JSON_PATH}")
    try:
        static_df = spark.read.format("json").load(BRONZE_JSON_PATH)
        bronze_schema = static_df.schema
    except Exception as e:
        print("Không tìm thấy file dữ liệu. Dừng.")
        sys.exit(0)

    # 3. Đọc Stream & Chọn Cột
    print("Đang đọc stream...")
    df_stream = spark.readStream.format("json") \
        .schema(bronze_schema) \
        .load(BRONZE_JSON_PATH) \
        .select(
            col("*"), 
            col("_metadata.file_modification_time").alias("file_modification_time") 
        )

    # 4. Ghi dữ liệu
    print(f"Đang ghi vào: {TABLE_LOCATION}")
    query = (
        df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(once=True)
        .start(TABLE_LOCATION) # Ghi vào đường dẫn vật lý
    )
    
    query.awaitTermination()
    print("Ghi dữ liệu xong.")

    # 5. Đăng ký với Hive Metastore
    print("Đang cập nhật Hive Metastore...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME}
        USING DELTA
        LOCATION '{TABLE_LOCATION}'
    """)
    spark.sql(f"REFRESH TABLE {FULL_TABLE_NAME}")
    print(f"Hoàn tất. Bảng {FULL_TABLE_NAME} đã sẵn sàng.")

if __name__ == "__main__":
    run_load_to_bronze_table()