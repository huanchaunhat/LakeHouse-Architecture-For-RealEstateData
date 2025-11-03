from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# =====================================================
# 1️⃣ Khởi tạo SparkSession với Delta + MinIO
# =====================================================
builder = (
    SparkSession.builder
    .appName("ReadFromMinIO_Delta")
    # Cấu hình Delta Lake
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Cấu hình MinIO (S3A)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# =====================================================
# 2️⃣ Đọc dữ liệu Delta từ MinIO
# =====================================================
silver_path = "s3a://lakehouse/silver/date=2025-10-15/crawl_cleaned_20251015_082620/Date=2025-10-15/"

df = spark.read.format("delta").load(silver_path)

# =====================================================
# 3️⃣ Hiển thị dữ liệu
# =====================================================
df.show(5)
df.printSchema()
