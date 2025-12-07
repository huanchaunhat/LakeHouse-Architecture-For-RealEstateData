# LakeHouse Architecture For Real Estate Data

## Tổng Quan Dự Án

Hệ thống Lakehouse hoàn chỉnh để thu thập, xử lý và phân tích dữ liệu bất động sản từ Chợ Tốt, sử dụng kiến trúc **Medallion** (Bronze-Silver-Gold).

## Kiến Trúc Hệ Thống

![Sơ đồ kiến trúc hệ thống](.image/kientruchethong.png)

## Quy Trình Hoạt Động
```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│                   Chợ Tốt API (Real Estate)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│              Airflow DAG (Daily Schedule)                        │
│              - Crawl API (Python)                                │
│              - Save to MinIO (S3)                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                       │
│              Storage: MinIO (S3-compatible)                      │
│              Format: JSON Lines (.jsonl)                         │
│              Location: s3a://lakehouse/bronze/json/              │
│              Spark: Load → Delta Table                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Clean Data)                       │
│              Tool: dbt + Spark Thrift Server                     │
│              Model: stg_properties                               │
│              - Deduplication (keep latest)                       │
│              - Parse numbers from text                           │
│              - Normalize address                                 │
│              - Data quality flag                                 │
│              Format: Delta Table                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Analytics)                         │
│              Tool: dbt Models                                    │
│              Tables:                                             │
│              - fct_properties (fact table)                       │
│              - fct_daily_summary (aggregated)                    │
│              - fct_data_quality_report (monitoring)              │
│              Format: Delta Table                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                   VISUALIZATION                                  │
│              Apache Superset (Connect via Spark Thrift)          │
│              - Dashboards                                        │
│              - Ad-hoc queries                                    │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component          | Technology     | Purpose                        |
| ------------------ | -------------- | ------------------------------ |
| **Orchestration**  | Apache Airflow | Schedule & monitor pipelines   |
| **Storage**        | MinIO (S3)     | Object storage for data lake   |
| **Processing**     | Apache Spark   | Distributed data processing    |
| **Catalog**        | Hive Metastore | Metadata management            |
| **Transformation** | dbt            | SQL-based transformations      |
| **Format**         | Delta Lake     | ACID transactions, time travel |
| **Visualization**  | Apache Superset| BI dashboards                  |
| **Container**      | Docker Compose | Infrastructure as code         |

## Cấu Trúc Dự Án

```
.
├── airflow/                    # Airflow configuration
│   ├── dags/
│   │   └── end_to_end_pipeline.py  # Main ELT pipeline
│   ├── jars/                   # Spark dependencies
│   └── config/
├── dbt/                        # dbt project
│   ├── models/
│   │   ├── staging/            # Silver layer
│   │   │   └── stg_properties.sql
│   │   └── marts/              # Gold layer
│   │       ├── fct_properties.sql
│   │       ├── fct_daily_summary.sql
│   │       └── fct_data_quality_report.sql
│   └── DATA_QUALITY_GUIDE.md
├── spark/                      # Spark configuration
├── scripts/                    # Utility scripts
│   └── load_bronze_to_table.py
├── hive-metastore/            # Metastore config
└── docker-compose.yml         # Infrastructure definition
```

## Khởi Động Hệ Thống

### 1. Start Services

```bash
docker-compose up -d
```

### 2. Kiểm Tra Services

```bash
# Airflow
http://localhost:8088

# Spark Master
http://localhost:8081

# MinIO Console
http://localhost:9001

# Metabase
http://localhost:3000
```

### 3. Chạy Pipeline

```bash
# Create connection
docker exec airflow-scheduler airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' \
    --conn-extra '{"deploy-mode": "client"}' 2>&1 | tail -5

# Trigger DAG manually
docker exec airflow-webserver airflow dags trigger end_to_end_lakehouse_pipeline

# Hoặc đợi schedule (daily)
```

## Data Flow

### 1. **Bronze Layer** (Raw Ingestion)

- **Input**: Chợ Tốt API responses
- **Process**: Crawl → Save to MinIO as JSONL
- **Output**: `s3a://lakehouse/bronze/json/crawl_*.jsonl`
- **Table**: `bronze.raw_properties`

### 2. **Silver Layer** (Data Cleaning)

- **Input**: `bronze.raw_properties`
- **Process**:
  - Deduplication (latest record per property_id)
  - Parse numbers from Vietnamese text
  - Normalize price (convert to billions VNĐ)
  - Normalize address (proper case)
  - Add data quality flags
- **Output**: `silver.stg_properties`
- **Key Improvements**:

### 3. **Gold Layer** (Analytics Ready)

- **Input**: `silver.stg_properties`
- **Process**:
  - Filter only VALID records
  - Remove outliers
  - Calculate metrics (price per m²)
  - Aggregate summaries
- **Output**:
  - `gold.fct_properties` - Main fact table
  - `gold.fct_daily_summary` - Daily aggregations
  - `gold.fct_data_quality_report` - Quality monitoring

## Visualization Layer - Apache Superset

### Superset Connection Options

**Spark Thrift Server**

```yaml
Connection Type: Apache Hive
SQLAlchemy URI: hive://spark-thrift-server:10000?auth=NOSASL
```

### Quick Start with Superset

```bash
# 1. Start Superset
docker-compose up -d superset

# 2. Create admin user
docker exec -it superset bash
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin
exit

3. Access UI
Open: http://localhost:8089
Login: admin / admin

4. Add Hive database connection
Settings > Database Connections > + Database
Type: Apache Hive
URI: hive://spark-thrift-server:10000?auth=NOSASL

```

## Contributors

Data Engineering Team

## License

MIT License

---

**Last Updated:** November 21, 2025
**Version:** 2.0
