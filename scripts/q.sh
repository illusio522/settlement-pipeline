#!/usr/bin/env bash
# 한 줄 쿼리 실행
# 사용: bash scripts/q.sh "SELECT COUNT(*) FROM payment"
cd "$(dirname "$0")/.."
echo "$*" | docker run --rm -i -v "$PWD":/work settlement-duckdb:1.5.3 /work/data/settlement.duckdb
