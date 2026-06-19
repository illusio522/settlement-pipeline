#!/usr/bin/env bash
# 3-2: Iceberg 테이블(MinIO) 기준 정산 재구성 → e2e_iceberg.duckdb
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
NET=settlement-pipeline_default
rm -f data/e2e_iceberg.duckdb
echo "▶ Iceberg 기준 정산 재구성 (MinIO Iceberg→canonical→정산)"
cat transform/01_schema.sql \
    transform/e2e_load_iceberg.sql \
    transform/03_settlement.sql \
    transform/04_checks.sql \
  | docker run --rm -i --network "$NET" -v "$PWD":/work "$IMG" /work/data/e2e_iceberg.duckdb
echo "✅ 완료 — data/e2e_iceberg.duckdb"
