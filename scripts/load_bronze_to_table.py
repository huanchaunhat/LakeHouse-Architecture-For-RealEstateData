#!/usr/bin/env python3
"""
Load Bronze Layer - Incremental Processing
Chi xu ly cac file JSON chua duoc xu ly
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name
from delta import configure_spark_with_delta_pip
import boto3
from botocore.client import Config

# Configuration
BUCKET = "lakehouse"
BRONZE_JSON_PATH = f"s3a://{BUCKET}/bronze/json/"
PROCESSED_FILES_LIST = "bronze/_processed_files.txt"

DATABASE_NAME = "bronze"
TABLE_NAME = "raw_properties"
FULL_TABLE_NAME = f"{DATABASE_NAME}.{TABLE_NAME}"
TABLE_LOCATION = f"s3a://{BUCKET}/warehouse/{DATABASE_NAME}.db/{TABLE_NAME}"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def get_processed_files():
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=BUCKET, Key=PROCESSED_FILES_LIST)
        content = response['Body'].read().decode('utf-8')
        processed = set(line.strip() for line in content.split('\n') if line.strip())
        print(f"Found {len(processed)} processed files")
        return processed
    except s3.exceptions.NoSuchKey:
        print("No processed files yet (first run)")
        return set()
    except Exception as e:
        print(f"Error reading processed files: {e}")
        return set()

def get_all_json_files():
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix='bronze/json/')
        if 'Contents' not in response:
            return []
        files = [obj['Key'] for obj in response['Contents'] 
                if obj['Key'].endswith('.jsonl') or obj['Key'].endswith('.json')]
        print(f"Found {len(files)} JSON files in MinIO")
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def save_processed_files(file_list):
    s3 = get_s3_client()
    try:
        content = '\n'.join(sorted(file_list))
        s3.put_object(Bucket=BUCKET, Key=PROCESSED_FILES_LIST, Body=content.encode('utf-8'))
        print(f"Saved {len(file_list)} files to processed list")
    except Exception as e:
        print(f"Error saving processed files: {e}")

def run_load_to_bronze_table():
    print("=" * 50)
    print("Bronze Layer - Incremental Load")
    print("=" * 50)
    
    processed_files = get_processed_files()
    all_files = get_all_json_files()
    
    if not all_files:
        print("No files to process")
        return
    
    new_files = [f for f in all_files if f not in processed_files]
    
    # Initialize Spark session first (needed for table registration)
    builder = (
        SparkSession.builder
        .appName("LoadBronzeIncremental")
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
    
    # Register existing table if data already exists in MinIO (check _delta_log folder)
    s3 = get_s3_client()
    delta_log_key = f"warehouse/{DATABASE_NAME}.db/{TABLE_NAME}/_delta_log/"
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=delta_log_key, MaxKeys=1)
        if response.get('KeyCount', 0) > 0:
            # Delta log exists, register table
            spark.sql(f"CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} USING DELTA LOCATION '{TABLE_LOCATION}'")
            print(f"✅ Table {FULL_TABLE_NAME} registered with existing data")
        else:
            print("⚠️  No existing Delta table data")
    except Exception as e:
        print(f"⚠️  Could not check for existing data: {e}")
    
    # Check if we have new files to process
    if not new_files:
        print("All files already processed. Skip loading new data.")
        spark.stop()
        return
    
    print(f"\nDetected {len(new_files)} NEW files to process:")
    for f in new_files[:5]:
        print(f"  - {f}")
    if len(new_files) > 5:
        print(f"  ... and {len(new_files) - 5} more")
    
    print(f"\nReading and filtering new files...")
    
    try:
        static_df = spark.read.format("json").load(BRONZE_JSON_PATH)
        bronze_schema = static_df.schema
        
        df_all = spark.read.format("json").schema(bronze_schema).load(BRONZE_JSON_PATH)
        df_all = df_all.withColumn("_input_file", input_file_name())
        
        processed_files_s3a = [f"s3a://{BUCKET}/{f}" for f in processed_files]
        df_new = df_all.filter(~col("_input_file").isin(processed_files_s3a))
        
        df_new = df_new.select(
            col("*"),
            col("_metadata.file_modification_time").alias("file_modification_time")
        ).drop("_input_file")
        
        row_count = df_new.count()
        
        if row_count == 0:
            print("No new records. Skip.")
            spark.stop()
            return
        
        print(f"Detected {row_count} new records")
        print(f"\nWriting {row_count} records to Bronze table...")
        
        table_exists = spark.catalog._jcatalog.tableExists(DATABASE_NAME, TABLE_NAME)
        
        if table_exists:
            print("  Table exists, APPENDING new data...")
            df_new.write.format("delta").mode("append").option("mergeSchema", "true").save(TABLE_LOCATION)
        else:
            print("  Creating new table...")
            df_new.write.format("delta").mode("overwrite").save(TABLE_LOCATION)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} USING DELTA LOCATION '{TABLE_LOCATION}'")
        
        print(f"\nUpdating processed files list...")
        all_processed = processed_files.union(set(new_files))
        save_processed_files(all_processed)
        
        spark.sql(f"REFRESH TABLE {FULL_TABLE_NAME}")
        
        total_records = spark.sql(f"SELECT COUNT(*) as cnt FROM {FULL_TABLE_NAME}").collect()[0]['cnt']
        print(f"\nCOMPLETED!")
        print(f"  - New files processed: {len(new_files)}")
        print(f"  - New records: {row_count}")
        print(f"  - Total records in table: {total_records}")
        
        spark.stop()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_load_to_bronze_table()
