#!/usr/bin/env bash
# 3-3: dbt 실행. run=모델 생성 / test=테스트 / build=run+test 한번에(DAG 순서).
#   run·build 만 DB 초기화 (test는 기존 테이블 검사하므로 삭제 금지).
set -euo pipefail
cd "$(dirname "$0")/.."
CMD="${1:-build}"
case "$CMD" in run|build) rm -f data/dbt.duckdb ;; esac
docker run --rm -v "$PWD":/work -w /work/dbt settlement-dbt:1 \
  "$CMD" --profiles-dir . 2>&1
