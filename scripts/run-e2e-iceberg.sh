#!/usr/bin/env bash
# 3-2: Iceberg 테이블(MinIO) 기준 정산 재구성 → e2e_iceberg.duckdb
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f infra/.env ] || { echo "infra/.env 없음 — infra/.env.example 를 복사해 만드세요"; exit 1; }
set -a; . infra/.env; set +a
IMG=settlement-duckdb:1.5.3
NET=settlement-pipeline_default
rm -f data/e2e_iceberg.duckdb
echo "▶ Iceberg 기준 정산 재구성 (MinIO Iceberg→canonical→정산)"
{
# S3 접속은 환경변수에서 주입 (printf는 bash 내장 → ps에 자격증명 노출 없음)
printf "INSTALL httpfs; LOAD httpfs;\n"
printf "SET s3_endpoint='sp-minio:9000'; SET s3_access_key_id='%s';\n" "$MINIO_ROOT_USER"
printf "SET s3_secret_access_key='%s'; SET s3_use_ssl=false; SET s3_url_style='path';\n" "$MINIO_ROOT_PASSWORD"
cat transform/01_schema.sql \
    transform/e2e_load_iceberg.sql \
    transform/03_settlement.sql \
    transform/04_checks.sql
} | docker run --rm -i --network "$NET" -v "$PWD":/work "$IMG" /work/data/e2e_iceberg.duckdb
echo "✅ 완료 — data/e2e_iceberg.duckdb"
