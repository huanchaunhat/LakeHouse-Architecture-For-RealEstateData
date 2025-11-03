#!/usr/bin/env python3
"""
🏆 Gold Pipeline - từ Silver (Parquet) sang Gold (Delta)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, sum as _sum, when, lit, input_file_name, regexp_extract
from delta import configure_spark_with_delta_pip
from datetime import datetime

# =====================================================
# 1️⃣ Khởi tạo SparkSession (với Delta Lake)
# =====================================================
builder = (
    SparkSession.builder.appName("GoldPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# =====================================================
# 2️⃣ Đọc dữ liệu từ Silver
# =====================================================
silver_path = "s3a://lakehouse/silver/date=*/crawl_cleaned_*/Date=*/"
print("📂 Đọc dữ liệu Silver từ:", silver_path)

df_silver = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(silver_path)
)

print("✅ Dữ liệu Silver mẫu:")
df_silver.show(5)

# =====================================================
# 3️⃣ Chuẩn hóa cột Date (nếu thiếu)
# =====================================================
# Thêm cột Date từ đường dẫn file nếu chưa có
if "Date" not in df_silver.columns:
    df_silver = (
        df_silver.withColumn("file_path", input_file_name())
        .withColumn("Date", regexp_extract(col("file_path"), r"Date=(\d{4}-\d{2}-\d{2})", 1))
        .drop("file_path")
    )

# Nếu vẫn chưa có Date, gán theo ngày hiện tại
if "Date" not in df_silver.columns or df_silver.filter(col("Date") == "").count() > 0:
    today = datetime.now().strftime("%Y-%m-%d")
    df_silver = df_silver.withColumn("Date", lit(today))

# =====================================================
# 4️⃣ Làm sạch & chuyển đổi dữ liệu
# =====================================================
if "amount" in df_silver.columns:
    df_gold = (
        df_silver.groupBy("Date")
        .agg(
            count("*").alias("num_records"),
            _sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        .withColumn("etl_status", lit("success"))
    )
elif "Price" in df_silver.columns:
    df_gold = (
        df_silver.groupBy("Date")
        .agg(
            count("*").alias("num_records"),
            _sum("Price").alias("total_price"),
            avg("Price").alias("avg_price")
        )
        .withColumn("etl_status", lit("success"))
    )
else:
    df_gold = df_silver.withColumn("etl_status", lit("cleaned"))

print("✅ Dữ liệu Gold sau xử lý:")
df_gold.show(5)

# =====================================================
# 5️⃣ Ghi dữ liệu ra tầng Gold (Delta)
# =====================================================
gold_path = "s3a://lakehouse/gold/date_partitioned"
(
    df_gold.write.format("delta")
    .mode("overwrite")
    .partitionBy("Date")
    .save(gold_path)
)

print("🏁 Đã ghi dữ liệu ra tầng Gold:", gold_path)

# =====================================================
# 6️⃣ (Tùy chọn) Tạo bảng Gold trong Spark SQL
# =====================================================
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_table
    USING DELTA
    LOCATION '{gold_path}'
""")

print("📊 Bảng gold_table đã sẵn sàng!")

spark.stop()
