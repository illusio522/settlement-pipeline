#!/usr/bin/env bash
# 3-4: 증분 정산 1배치 실행 (반복 호출). 랜딩에서 watermark 이후만 처리.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
echo "▶ 증분 배치 실행 ($(date '+%H:%M:%S'))"
cat transform/inc_run.sql \
  | docker run --rm -i -v "$PWD":/work "$IMG" /work/data/incremental.duckdb 2>&1 | grep -vE '^[┌└├]'
