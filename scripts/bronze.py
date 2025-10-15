#!/usr/bin/env python3
"""
Bronze Pipeline - Crawl raw data và lưu vào MinIO (Bronze Layer)
"""

import requests
import time
import json
from datetime import datetime
import boto3
from io import BytesIO
import os
from botocore.exceptions import NoCredentialsError, ClientError

# ==================== CONFIG ====================
BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 20
LIMIT = 20

# MinIO config
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"

# File lưu các list_id đã crawl
SEEN_FILE = f"{BRONZE_PREFIX}/list_ids.txt"


# ==================== MinIO Client ====================
def create_minio_client():
    """Tạo client boto3 kết nối MinIO"""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
        )
        # Tạo bucket nếu chưa có
        existing_buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
        if BUCKET not in existing_buckets:
            client.create_bucket(Bucket=BUCKET)
            print(f"Đã tạo bucket mới: {BUCKET}")
        return client
    except Exception as e:
        print(f"Lỗi khi khởi tạo MinIO client: {e}")
        raise


s3 = create_minio_client()


# ==================== FUNCTIONS ====================
def load_seen_ids():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=SEEN_FILE)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        return set(line.strip() for line in lines if line.strip())
    except s3.exceptions.NoSuchKey:
        print("Chưa có list_ids.txt trong MinIO, tạo mới.")
        return set()
    except Exception as e:
        print(f"Lỗi đọc list_ids.txt từ MinIO: {e}")
        return set()


def save_seen_ids(new_ids):
    try:
        existing = load_seen_ids()
        all_ids = existing.union(new_ids)
        data_bytes = BytesIO("\n".join(all_ids).encode("utf-8"))
        s3.put_object(Bucket=BUCKET, Key=SEEN_FILE, Body=data_bytes)
        print(f"Cập nhật {len(new_ids)} ID mới vào {SEEN_FILE}")
    except Exception as e:
        print(f"Lỗi lưu list_ids.txt lên MinIO: {e}")

def fetch_list(offset=0, limit=20):
    """Lấy danh sách quảng cáo từ Chợ Tốt"""
    try:
        params = {"cg": 1000, "o": offset, "limit": limit}
        r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("ads", [])
    except Exception as e:
        print(f"Lỗi khi fetch list (offset={offset}): {e}")
        return []


def fetch_detail(list_id, retries=3):
    """Lấy chi tiết từng quảng cáo, có retry"""
    for attempt in range(retries):
        try:
            r = requests.get(DETAIL_URL.format(list_id), headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            ad = data.get("ad", {})

            # Thông tin cơ bản
            result = {
                "list_id": list_id,
                "title": ad.get("subject"),
                "price": ad.get("price_string"),
                "address": ad.get("address") or None,
                "images": ad.get("images", []),
            }

            # Các thông số trong parameters
            for p in data.get("parameters", []):
                if "label" in p and "value" in p:
                    result[p["label"]] = p["value"]

            return result
        except Exception as e:
            print(f"Lỗi {list_id} (lần {attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)
    return None


def upload_to_minio(data_bytes, key):
    """Upload file JSON vào MinIO"""
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=data_bytes)
        print(f"Upload thành công lên MinIO: {key}")
    except (NoCredentialsError, ClientError) as e:
        print(f"Lỗi upload lên MinIO: {e}")
        raise


def run_bronze():
    """Main pipeline"""
    seen_ids = load_seen_ids()
    new_ids = []
    all_details = []

    for p in range(1, MAX_PAGES + 1):
        offset = (p - 1) * LIMIT
        ads = fetch_list(offset, LIMIT)
        if not ads:
            break

        for ad in ads:
            list_id = str(ad.get("list_id"))
            if not list_id or list_id in seen_ids:
                continue

            detail = fetch_detail(list_id)
            if detail:
                all_details.append(detail)
                new_ids.append(list_id)

        print(f"Trang {p}: thu được {len(all_details)} bản ghi mới")
        time.sleep(0.2)

    if not all_details:
        print("Không có dữ liệu mới để lưu.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{BRONZE_PREFIX}crawl_{timestamp}.json"

    try:
        data_bytes = BytesIO(json.dumps(all_details, ensure_ascii=False).encode("utf-8"))
        upload_to_minio(data_bytes, key)
        save_seen_ids(new_ids)
        print(f"Đã lưu {len(all_details)} bản ghi mới vào {key}")
        print(f"Cập nhật {len(new_ids)} ID vào {SEEN_FILE}")
    except Exception as e:
        print(f"Lỗi trong quá trình lưu dữ liệu: {e}")


if __name__ == "__main__":
    run_bronze()