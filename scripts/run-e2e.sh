#!/usr/bin/env bash
# 2-5 작업3: 랜딩(NDJSON) 기준으로 정산 전 과정 재구성 → e2e.duckdb
#   01_schema(스키마) → e2e_load(마스터+랜딩 dedup) → 03_settlement → 04_checks
# 전제: 컨슈머가 data/landing/*.ndjson 을 이미 적재해 둔 상태.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
rm -f data/e2e.duckdb
echo "▶ E2E 정산 재구성 (랜딩→canonical→정산)"
cat transform/01_schema.sql \
    transform/e2e_load.sql \
    transform/03_settlement.sql \
    transform/04_checks.sql \
  | docker run --rm -i -v "$PWD":/work "$IMG" /work/data/e2e.duckdb
echo "✅ 완료 — data/e2e.duckdb"
