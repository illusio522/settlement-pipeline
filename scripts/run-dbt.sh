#!/usr/bin/env bash
# dbt 실행.
#   build / run      = DB 초기화 후 전체 빌드 (baseline / full-refresh). incremental 모델은 첫 실행=전체.
#   inc              = DB 유지하고 dbt run (★ incremental: _seq watermark로 새 랜딩 행만 처리).
#   test             = 기존 테이블 검사 (DB 삭제 금지).
# 전제: data/landing/*.p*.ndjson 존재. 결과 DB = data/dbt.duckdb.
set -euo pipefail
cd "$(dirname "$0")/.."
CMD="${1:-build}"
case "$CMD" in
  run|build) rm -f data/dbt.duckdb ;;          # 전체 빌드: 초기화
  inc)       CMD="run" ;;                        # 증분: 초기화 안 함(기존 테이블=watermark)
esac
docker run --rm -v "$PWD":/work -w /work/dbt settlement-dbt:1 \
  "$CMD" --profiles-dir . 2>&1
