#!/usr/bin/env bash
# 1단계 실행 — 도커 DuckDB로 01→02→03→04 파이프 (스키마→시드→정산→검증)
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
rm -f data/settlement.duckdb
echo "▶ DuckDB 정산 배치 실행 (스키마→시드→정산→검증)"
cat transform/01_schema.sql \
    generator/02_seed.sql \
    transform/03_settlement.sql \
    transform/04_checks.sql \
  | docker run --rm -i -v "$PWD":/work settlement-duckdb:1.5.3 /work/data/settlement.duckdb
echo "✅ 완료 — data/settlement.duckdb 생성됨 (재조회: docker run --rm -it -v \$PWD:/work settlement-duckdb:1.5.3 /work/data/settlement.duckdb)"
