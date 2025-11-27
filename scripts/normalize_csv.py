#!/usr/bin/env python3
"""
Load CSV vào Bronze (Giả lập JSON) - Phiên bản Sạch & Fix Lỗi Duplicate
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat, coalesce, split, element_at, trim
from delta import configure_spark_with_delta_pip

# ================= CONFIG =================
BUCKET = "lakehouse"
CSV_SOURCE_PATH = f"s3a://{BUCKET}/bronze/csv/vietnam_housing_dataset.csv"

DATABASE_NAME = "bronze"
TABLE_NAME = "raw_properties"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"
TARGET_DELTA_PATH = f"s3a://{BUCKET}/warehouse/{DATABASE_NAME}.db/{TABLE_NAME}"

SIMPLE_MAPPING = {
    "Frontage": "Chiều ngang",
    "Floors": "Tổng số tầng",
    "Bedrooms": "Số phòng ngủ",
    "Bathrooms": "Số phòng vệ sinh",
    "Legal status": "Giấy tờ pháp lý",
    "House direction": "Hướng cửa chính",
    "Price": "price"                    
}

# ================= SPARK SESSION =================
builder = (
    SparkSession.builder
    .appName("LoadCSV_Clean")
    .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
    .enableHiveSupport()
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

def run_load_csv():
    print(f" Bắt đầu load CSV vào bảng: {FULL_TABLE_NAME}")

    # 1. Đọc file CSV
    try:
        df_csv = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(CSV_SOURCE_PATH)
    except Exception as e:
        print(f" Lỗi đọc file CSV: {e}")
        sys.exit(1)

    print(" Đang chuẩn hóa dữ liệu...")
    
    select_exprs = []
    
    # --- NHÓM 1: CÁC CỘT TẠO MỚI (ID, Meta) ---
    # a. Tạo ID
    select_exprs.append(
        md5(concat(
            coalesce(col("Address"), lit("")), 
            coalesce(col("Price").cast("string"), lit("")),
            coalesce(col("Area").cast("string"), lit(""))
        )).alias("list_id")
    )

    select_exprs.append(col("Address").alias("Địa chỉ"))
    select_exprs.append(col("Address").alias("title"))
    select_exprs.append(current_timestamp().alias("file_modification_time"))
    select_exprs.append(lit("csv_import").alias("source_origin"))

    # --- NHÓM 2: MAPPING CỘT CÓ SẴN ---
    for csv_col, json_col in SIMPLE_MAPPING.items():
        if csv_col in df_csv.columns:
            # Ép về String hết để an toàn
            select_exprs.append(col(f"`{csv_col}`").cast("string").alias(json_col))
        else:
            select_exprs.append(lit(None).cast("string").alias(json_col))

    # --- NHÓM 3: XỬ LÝ ĐỊA CHỈ ---
    select_exprs.append(trim(element_at(split(col("Address"), ","), -1)).alias("Tỉnh, thành phố"))
    select_exprs.append(trim(element_at(split(col("Address"), ","), -2)).alias("Quận, Huyện"))
    select_exprs.append(trim(element_at(split(col("Address"), ","), -3)).alias("Phường, thị xã, thị trấn"))

    # --- NHÓM 4: XỬ LÝ DIỆN TÍCH ---
    if "Area" in df_csv.columns:
        select_exprs.append(col("Area").cast("string").alias("Diện tích"))
        select_exprs.append(col("Area").cast("string").alias("Diện tích đất"))
    else:
        select_exprs.append(lit(None).cast("string").alias("Diện tích"))
        select_exprs.append(lit(None).cast("string").alias("Diện tích đất"))

    select_exprs.append(lit(None).cast("string").alias("Diện tích sử dụng"))
    select_exprs.append(lit(None).cast("array<string>").alias("images"))


    df_final = df_csv.select(*select_exprs)

    print(f" Đang ghi {df_final.count()} dòng vào: {TARGET_DELTA_PATH}")
    
    table_exists = spark.catalog.tableExists(FULL_TABLE_NAME)
    
    (
        df_final.write
        .format("delta")
        .mode("append") # Luôn Append
        .option("mergeSchema", "true") 
        .save(TARGET_DELTA_PATH)
    )
    
    # Cập nhật Hive Metastore
    if not table_exists:
        print(f" Tạo bảng mới {FULL_TABLE_NAME}...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME}
            USING DELTA LOCATION '{TARGET_DELTA_PATH}'
        """)
    
    print(" Refreshing table...")
    spark.sql(f"REFRESH TABLE {FULL_TABLE_NAME}")
    print(" Hoàn tất!")

if __name__ == "__main__":
    run_load_csv()