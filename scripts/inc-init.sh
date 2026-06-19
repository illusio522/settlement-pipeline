#!/usr/bin/env bash
# 3-4: 증분 정산 상태 DB 초기화 (1회). data/incremental.duckdb 새로 만듦.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
rm -f data/incremental.duckdb
echo "▶ 증분 상태 DB 초기화 (스키마+마스터+watermark=-1)"
cat transform/01_schema.sql transform/inc_init.sql \
  | docker run --rm -i -v "$PWD":/work "$IMG" /work/data/incremental.duckdb
echo "✅ data/incremental.duckdb 준비 (데이터 비어있음, watermark -1)"
