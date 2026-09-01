#!/usr/bin/env bash
# 일 정산 산출(DuckDB e2e_iceberg) → 서빙 스토어(servedb) 재적재. TRUNCATE+COPY = 멱등.
#   DAG settlement_close 의 4번째 태스크. 마지막에 원본/서빙 건수 대조로 자체 검증.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f infra/.env ] || { echo "infra/.env 없음 — infra/.env.example 를 복사해 만드세요"; exit 1; }
set -a; . infra/.env; set +a
SRC=data/e2e_iceberg.duckdb

# servedb 없으면 생성 + DDL 멱등 적용
docker exec sp-postgres psql -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='servedb'" | grep -q 1 \
  || docker exec sp-postgres psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE servedb"
docker exec -i sp-postgres psql -U "$POSTGRES_USER" -d servedb -v ON_ERROR_STOP=1 \
  < infra/postgres/serving.sql >/dev/null

# DuckDB → CSV (임시) → 트랜잭션 안에서 TRUNCATE+COPY
docker run --rm -v "$PWD":/work settlement-duckdb:1.5.3 "/work/$SRC" -c \
  "COPY (SELECT settlement_date, partner_id, brand, method, gross, \"cancel\", net, fee, payout, txn_count
         FROM settlement_agg) TO '/work/data/serving_agg.csv' (FORMAT CSV, HEADER false);"
docker exec -i sp-postgres psql -U "$POSTGRES_USER" -d servedb -v ON_ERROR_STOP=1 \
  -c "BEGIN; TRUNCATE settlement_agg; COPY settlement_agg FROM STDIN (FORMAT csv); COMMIT;" \
  < data/serving_agg.csv
rm -f data/serving_agg.csv

# 자체 검증: 원본 vs 서빙 건수 일치해야 성공
SRC_CNT=$(docker run --rm -v "$PWD":/work settlement-duckdb:1.5.3 "/work/$SRC" -noheader -list -c "SELECT count(*) FROM settlement_agg;" | tr -d ' \r')
DST_CNT=$(docker exec sp-postgres psql -U "$POSTGRES_USER" -d servedb -tAc "SELECT count(*) FROM settlement_agg" | tr -d ' ')
[ "$SRC_CNT" = "$DST_CNT" ] || { echo "❌ 건수 불일치 원본=$SRC_CNT 서빙=$DST_CNT"; exit 1; }
echo "✅ 서빙 재적재 완료 — settlement_agg ${DST_CNT}행"
