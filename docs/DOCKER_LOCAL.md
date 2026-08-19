# Môi trường Docker local

## Yêu cầu

- Docker Engine 24+ và Docker Compose v2
- Tối thiểu 8 GB RAM; khuyến nghị 12 GB RAM và 4 CPU
- Các cổng trống: `3308`, `4040`, `7077`, `8081`, `8088`, `8089`, `9000`, `9001`, `9083`, `10000`

## Khởi động

```bash
make init
# Thay toàn bộ mật khẩu mẫu trong .env.local
make config
make up
make status
```

Nếu không có `make`:

```bash
cp .env.example .env.local
docker compose --env-file .env.local -f docker-compose.local.yml config --quiet
docker compose --env-file .env.local -f docker-compose.local.yml up -d --build
```

## Địa chỉ dịch vụ

| Dịch vụ | URL/cổng | Mục đích |
|---|---|---|
| MinIO API | `http://localhost:9000` | S3-compatible storage |
| MinIO Console | `http://localhost:9001` | Quản lý bucket Bronze/Silver/Gold |
| Spark Master | `http://localhost:8081` | Theo dõi Spark cluster |
| Spark Thrift | `localhost:10000` | Kết nối SQL từ Superset/dbt |
| Airflow | `http://localhost:8088` | Điều phối pipeline |
| Superset | `http://localhost:8089` | Phân tích và dashboard |
| Hive Metastore | `localhost:9083` | Catalog cho Delta/Spark |

Tài khoản Airflow, Superset và MinIO được lấy từ `.env.local`.

## Kiểm tra nhanh

```bash
make status
docker compose --env-file .env.local -f docker-compose.local.yml exec minio mc ready local
docker compose --env-file .env.local -f docker-compose.local.yml exec spark-master spark-submit --version
```

## Thêm Spark worker

Không đặt `container_name`, nên có thể scale worker:

```bash
docker compose --env-file .env.local -f docker-compose.local.yml up -d --scale spark-worker=2
```

## Dừng và xử lý lỗi

```bash
make logs
make down
```

Dữ liệu nằm trong named volumes và vẫn được giữ sau `make down`. Lệnh xóa volumes được cố ý không tự động hóa trong target `reset` để tránh mất dữ liệu ngoài ý muốn.

Nếu Docker Desktop bị thiếu RAM, giảm `SPARK_WORKER_MEMORY` xuống `1G` hoặc chỉ chạy một worker.
