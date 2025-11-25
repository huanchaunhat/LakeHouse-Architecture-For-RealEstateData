#!/usr/bin/env python3
"""
Normalize column names - INCREMENTAL MODE
Chi xu ly record moi, giu lai data cu
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, desc
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

COLUMN_MAPPING = {
    "list_id": "list_id",
    "title": "title",
    "price": "price",
    "images": "images",
    "file_modification_time": "file_modification_time",
    "Diện tích đất": "land_area_raw",
    "Diện tích": "area_raw",
    "Diện tích sử dụng": "usable_area_raw",
    "Chiều ngang": "frontage_raw",
    "Chiều dài": "length_raw",
    "Tổng số tầng": "total_floors_raw",
    "Tầng số": "floor_number_raw",
    "Số phòng ngủ": "bedrooms_raw",
    "Số phòng vệ sinh": "bathrooms_raw",
    "Giấy tờ pháp lý": "legal_status_raw",
    "Tình trạng": "status_raw",
    "Tình trạng bất động sản": "property_status_raw",
    "Tình trạng nội thất": "furniture_status_raw",
    "Hướng cửa chính": "house_direction_raw",
    "Hướng ban công": "balcony_direction_raw",
    "Hướng đất": "land_direction_raw",
    "Nội thất": "furniture_raw",
    "Căn góc": "corner_unit_raw",
    "Đặc điểm nhà/đất": "property_features_raw",
    "Địa chỉ": "address", 
    "Phường, thị xã, thị trấn": "ward_raw",
    "Quận, Huyện": "district_raw",
    "Tỉnh, thành phố": "province_raw",
    "Loại hình căn hộ": "apartment_type_raw",
    "Loại hình nhà ở": "house_type_raw",
    "Loại hình đất": "land_type_raw",
    "Loại hình văn phòng": "office_type_raw",
    "Mã căn": "unit_code_raw",
    "Tên phân khu": "subdivision_name_raw",
    "Tên phân khu/lô": "subdivision_lot_raw",
    "Số tiền cọc": "deposit_amount_raw",
    "Đơn vị (m2/hecta)": "unit_measurement_raw",
}

def normalize_column_names():
    builder = (
        SparkSession.builder
        .appName("NormalizeBronzeColumns_Incremental")
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
        .enableHiveSupport()
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    SOURCE_TABLE = "bronze.raw_properties"
    SOURCE_LOCATION = "s3a://lakehouse/warehouse/bronze.db/raw_properties"
    NORMALIZED_TABLE = "bronze.properties"
    NORMALIZED_LOCATION = "s3a://lakehouse/warehouse/bronze.db/properties"
    
    print("=" * 50)
    print("Normalize Bronze Columns - Incremental")
    print("=" * 50)
    
    # Check if source table exists first
    try:
        spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")
        print(f"\nSource table exists: {SOURCE_TABLE}")
    except:
        print(f"\nSource table does not exist: {SOURCE_TABLE}")
        print("Skipping normalization (table will be created when data is loaded)")
        spark.stop()
        return
    
    print(f"\nReading source table: {SOURCE_TABLE}")
    df_source = spark.read.format("delta").load(SOURCE_LOCATION)
    
    current_columns = df_source.columns
    print(f"Source columns: {len(current_columns)}")
    
    # Build column mapping with deduplication
    # FIX: Skip duplicate columns instead of adding suffix (_1, _2...)
    SKIP_COLUMNS = ["address"]  # Skip API address (always NULL)
    
    final_mapping = {}
    used_names = set()
    renamed_count = 0
    skipped_duplicates = 0
    skipped_explicit = 0
    
    for old_col in current_columns:
        # Explicitly skip unwanted columns
        if old_col in SKIP_COLUMNS:
            skipped_explicit += 1
            continue
        
        # Check if column has explicit mapping
        if old_col in COLUMN_MAPPING:
            new_name = COLUMN_MAPPING[old_col]
            if old_col != new_name:
                renamed_count += 1
        else:
            # Auto-generate safe name
            new_name = old_col.replace(" ", "_").replace(",", "").replace(".", "").replace("/", "_").lower()
        
        # Skip duplicates (keep only first occurrence)
        if new_name in used_names:
            skipped_duplicates += 1
            continue
        
        final_mapping[old_col] = new_name
        used_names.add(new_name)
    
    print(f"Renamed {renamed_count} columns, skipped {skipped_duplicates} duplicates, skipped {skipped_explicit} unwanted")
    
    # Apply all renames at once using select with aliases
    select_exprs = [col(old_name).alias(new_name) for old_name, new_name in final_mapping.items()]
    df_renamed = df_source.select(*select_exprs)
    
    # Deduplicate by list_id (keep latest by file_modification_time)
    window_spec = Window.partitionBy("list_id").orderBy(desc("file_modification_time"))
    df_renamed = df_renamed.withColumn("_row_num", row_number().over(window_spec)) \
                           .filter(col("_row_num") == 1) \
                           .drop("_row_num")
    
    print(f"Deduplicated by list_id")
    
    # Check if normalized table exists
    try:
        delta_table = DeltaTable.forPath(spark, NORMALIZED_LOCATION)
        table_exists = True
        print(f"\nNormalized table exists, using MERGE...")
    except:
        table_exists = False
        print(f"\nNormalized table does not exist, creating...")
    
    if table_exists:
        # MERGE: Update existing records, insert new ones
        delta_table.alias("target").merge(
            df_renamed.alias("source"),
            "target.list_id = source.list_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        print("MERGE completed (updated existing + inserted new)")
    else:
        # First time: overwrite to create table
        df_renamed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(NORMALIZED_LOCATION)
        print("Table created")
    
    # Always register table in Hive Metastore (in case it's not registered yet)
    print(f"\nRegistering table in Hive Metastore...")
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {NORMALIZED_TABLE} USING DELTA LOCATION '{NORMALIZED_LOCATION}'")
        print(f"Table registered: {NORMALIZED_TABLE}")
    except Exception as e:
        print(f"Warning: Failed to create table (might already exist): {e}")
    
    # Force refresh to sync metadata
    try:
        spark.sql(f"REFRESH TABLE {NORMALIZED_TABLE}")
        print(f"Table refreshed: {NORMALIZED_TABLE}")
    except Exception as e:
        print(f"Warning: Failed to refresh table: {e}")
    
    # Verify table exists and get count
    try:
        total_records = spark.sql(f"SELECT COUNT(*) as cnt FROM {NORMALIZED_TABLE}").collect()[0]['cnt']
        print(f"\nCOMPLETED!")
        print(f"  - Total records in normalized table: {total_records}")
    except Exception as e:
        print(f"Error: Cannot query table {NORMALIZED_TABLE}: {e}")
        # Try querying directly from Delta location as fallback
        total_records = spark.read.format("delta").load(NORMALIZED_LOCATION).count()
        print(f"\nCOMPLETED (queried from Delta location)!")
        print(f"  - Total records in normalized table: {total_records}")
    
    spark.stop()

if __name__ == "__main__":
    normalize_column_names()
