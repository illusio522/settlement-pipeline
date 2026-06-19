#!/usr/bin/env bash
# 대화형 DuckDB 셸 (data/settlement.duckdb 열기)
# 사용: bash scripts/duckdb-shell.sh
#   - SQL은 세미콜론(;)으로 끝내기
#   - .tables  테이블 목록 / .schema payment  스키마 / .quit  종료
cd "$(dirname "$0")/.."
docker run --rm -it -v "$PWD":/work settlement-duckdb:1.5.3 /work/data/settlement.duckdb
