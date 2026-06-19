#!/usr/bin/env bash
# 3-1: 로컬 랜딩(NDJSON) → MinIO 레이크(Parquet) 적재.
#   _op·_seq 보존(읽기 시점 dedup 위해). s3://lake/landing/<table>.parquet
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
NET=settlement-pipeline_default

echo "▶ 랜딩 NDJSON → MinIO Parquet"
cat <<'SQL' | docker run --rm -i --network "$NET" -v "$PWD":/work "$IMG" :memory:
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='sp-minio:9000'; SET s3_access_key_id='minio';
SET s3_secret_access_key='minio12345'; SET s3_use_ssl=false; SET s3_url_style='path';
COPY (SELECT * FROM read_json_auto('/work/data/landing/payment.p*.ndjson'))        TO 's3://lake/landing/payment.parquet'        (FORMAT PARQUET);
COPY (SELECT * FROM read_json_auto('/work/data/landing/payment_method.p*.ndjson')) TO 's3://lake/landing/payment_method.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_json_auto('/work/data/landing/order_item.p*.ndjson'))     TO 's3://lake/landing/order_item.parquet'     (FORMAT PARQUET);
COPY (SELECT * FROM read_json_auto('/work/data/landing/cancel.p*.ndjson'))         TO 's3://lake/landing/cancel.parquet'         (FORMAT PARQUET);
SQL

echo "▶ MinIO 적재 확인"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c "
  mc alias set local http://sp-minio:9000 minio minio12345 >/dev/null 2>&1 &&
  mc ls -r local/lake/landing/
"
echo "✅ MinIO 레이크 적재 완료"
