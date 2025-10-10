from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read from MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Đọc file CSV từ MinIO
df = spark.read.csv("s3a://test/vietnam_housing_dataset-2.csv", header=True, inferSchema=True)

df.show(5)
df.printSchema()
