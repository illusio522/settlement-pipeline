#!/usr/bin/env bash
# 파일 배치 단계별 성능 측정 (추출 → 입력 고정 → 검사 → dbt → 게시).
#   전용 Compose 프로젝트 settlement-file-bench에 PostgreSQL을 새로 띄우고, 끝나면 볼륨까지 정리한다.
#   사용: bash bench/file-batch-bench.sh "100000 1000000"   (값 = 생성할 전체 주문 수)
#   BENCH_DAYS(기본 2): 결제를 대상일 포함 며칠에 분산할지. 취소(전체 주문의 약 5%)는 모두 대상일에 둔다.
#   BENCH_PG_PORT(기본 55450): 호스트 포트.
set -euo pipefail
cd "$(dirname "$0")/.."
SCALES="${1:-100000 1000000}"
PYTHON="${FILE_BATCH_PYTHON:-.venv-file-batch/bin/python}"
export POSTGRES_USER=file_batch_bench
# 실행마다 새로 만드는 일회용 로컬 비밀번호. 출력하지 않는다.
POSTGRES_PASSWORD="$(openssl rand -hex 16)"
export POSTGRES_PASSWORD
export FILE_BATCH_PG_PORT="${BENCH_PG_PORT:-55450}"
COMPOSE=(docker compose -p settlement-file-bench -f infra/file-batch/compose.yml)
OUT="data/file-batch/bench/$(date +%Y%m%dT%H%M%S)"

# 이 벤치 전용 프로젝트만 정리한다. 기존 settlement-* 프로젝트는 건드리지 않는다.
"${COMPOSE[@]}" down -v >/dev/null 2>&1
trap '"${COMPOSE[@]}" down -v >/dev/null 2>&1' EXIT
"${COMPOSE[@]}" up -d --wait postgres

BASE="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${FILE_BATCH_PG_PORT}"
export FILE_BATCH_PAY_DSN="$BASE/paydb" FILE_BATCH_ORDER_DSN="$BASE/orderdb" FILE_BATCH_SERVING_DSN="$BASE/servedb"

for N in $SCALES; do
  PYTHONPATH="$PWD" "$PYTHON" bench/file_batch_bench.py \
    --orders "$N" --days "${BENCH_DAYS:-2}" --out "$OUT/orders=$N"
done
echo "reports: $OUT"
