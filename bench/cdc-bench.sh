#!/usr/bin/env bash
# CDC 경로 단계별 성능 측정. bench/file-batch-bench.sh와 같은 데이터·규모·DuckDB 스레드로 비교한다.
#   PostgreSQL -> Debezium -> Kafka -> 기존 Java 컨슈머 -> 랜딩 -> 기존 정산 SQL -> servedb 게시.
#   규모마다 전용 Compose 프로젝트 settlement-cdc-bench를 새로 띄우고(커넥터·오프셋 초기화), 끝나면 볼륨까지 정리한다.
#   사용: bash bench/cdc-bench.sh "100000 1000000"   (값 = 생성할 전체 주문 수)
#   필요: Docker, .venv-file-batch, 빌드된 normalize bootJar, 비어 있는 호스트 포트 55440~55444.
set -euo pipefail
cd "$(dirname "$0")/.."
SCALES="${1:-100000 1000000}"
PYTHON="${FILE_BATCH_PYTHON:-.venv-file-batch/bin/python}"
PROJECT=settlement-cdc-bench
# compose.cdc-test.yml이 고정한 로컬 전용 테스트 계정이다. 포트는 127.0.0.1에만 열린다.
export POSTGRES_USER=file_batch_test POSTGRES_PASSWORD=file_batch_test_local
COMPOSE=(docker compose -p "$PROJECT" -f infra/file-batch/compose.yml -f infra/file-batch/compose.cdc-test.yml)
OUT="data/file-batch/bench-cdc/$(date +%Y%m%dT%H%M%S)"

for port in 55440 55441 55442 55443 55444; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is in use; stop the process or stack using it first" >&2
    exit 1
  fi
done

# 이 벤치 전용 프로젝트만 정리한다. 기존 settlement-* 프로젝트는 건드리지 않는다.
trap '"${COMPOSE[@]}" down -v >/dev/null 2>&1' EXIT
export CDC_BENCH_PG_DSN="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:55440"
for N in $SCALES; do
  "${COMPOSE[@]}" down -v >/dev/null 2>&1
  "${COMPOSE[@]}" up -d --wait postgres kafka apicurio connect >/dev/null 2>&1
  PYTHONPATH="$PWD:$PWD/bench" "$PYTHON" bench/cdc_bench.py \
    --orders "$N" --days "${BENCH_DAYS:-2}" --out "$OUT/orders=$N" --project "$PROJECT"
done
echo "reports: $OUT"
