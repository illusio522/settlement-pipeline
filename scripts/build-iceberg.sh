#!/usr/bin/env bash
# 3-2: 랜딩(dedup canonical) → MinIO Iceberg 테이블 4개.
#   COPY ... (FORMAT ICEBERG) — 카탈로그 없이 파일시스템 기반(version-hint).
#   event_time epoch_ms→TIMESTAMP 변환, 읽기 dedup 적용한 canonical을 적재.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
NET=settlement-pipeline_default

echo "▶ 기존 iceberg 테이블 정리"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c "
  mc alias set local http://sp-minio:9000 minio minio12345 >/dev/null 2>&1
  mc rm -r --force local/lake/iceberg >/dev/null 2>&1 || true
"

echo "▶ canonical → Iceberg 테이블 쓰기"
cat <<'SQL' | docker run --rm -i --network "$NET" -v "$PWD":/work "$IMG" :memory:
INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
SET s3_endpoint='sp-minio:9000'; SET s3_access_key_id='minio';
SET s3_secret_access_key='minio12345'; SET s3_use_ssl=false; SET s3_url_style='path';

COPY (
  SELECT txn_id, order_no, brand, amount, type, orig_txn_id, epoch_ms(event_time) AS event_time
  FROM (SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) rn
        FROM read_json_auto('/work/data/landing/payment.p*.ndjson')) WHERE rn=1 AND _op<>'d'
) TO 's3://lake/iceberg/payment' (FORMAT ICEBERG);

COPY (
  SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, epoch_ms(event_time) AS event_time
  FROM (SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) rn
        FROM read_json_auto('/work/data/landing/payment_method.p*.ndjson')) WHERE rn=1 AND _op<>'d'
) TO 's3://lake/iceberg/payment_method' (FORMAT ICEBERG);

COPY (
  SELECT order_no, line_no, partner_id, brand, item_amount
  FROM (SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) rn
        FROM read_json_auto('/work/data/landing/order_item.p*.ndjson')) WHERE rn=1 AND _op<>'d'
) TO 's3://lake/iceberg/order_item' (FORMAT ICEBERG);

COPY (
  SELECT cancel_txn_id, order_no, line_no
  FROM (SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) rn
        FROM read_json_auto('/work/data/landing/cancel.p*.ndjson')) WHERE rn=1 AND _op<>'d'
) TO 's3://lake/iceberg/cancel' (FORMAT ICEBERG);
SQL

echo "▶ Iceberg 구조 확인 (metadata/data/snapshot)"
docker run --rm --network "$NET" --entrypoint sh minio/mc -c "
  mc alias set local http://sp-minio:9000 minio minio12345 >/dev/null 2>&1
  echo '  [payment 테이블 구조]'
  mc ls -r local/lake/iceberg/payment/ | awk '{print \"    \"\$NF}'
"
echo "✅ Iceberg 테이블 4개 생성 완료"
