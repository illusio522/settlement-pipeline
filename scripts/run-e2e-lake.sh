#!/usr/bin/env bash
# 3-1: MinIO 레이크(Parquet) 기준 정산 재구성 → e2e_lake.duckdb
#   01_schema → e2e_load_lake(s3 읽기) → 03_settlement → 04_checks
# DuckDB 컨테이너를 compose 네트워크에 띄워 sp-minio 접근.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
NET=settlement-pipeline_default
rm -f data/e2e_lake.duckdb
echo "▶ 레이크 기준 정산 재구성 (MinIO→canonical→정산)"
cat transform/01_schema.sql \
    transform/e2e_load_lake.sql \
    transform/03_settlement.sql \
    transform/04_checks.sql \
  | docker run --rm -i --network "$NET" -v "$PWD":/work "$IMG" /work/data/e2e_lake.duckdb
echo "✅ 완료 — data/e2e_lake.duckdb"
