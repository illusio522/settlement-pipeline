#!/usr/bin/env bash
# 4-A: 단일 노드(DuckDB) 규모별 성능 측정.
#   규모별로: ① 생성(seed) ② 정산(03) 을 분리 계측. 처리시간·행수·초당건수 리포트.
#   사용: bash bench/run-bench.sh "100000 1000000 10000000"
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
SCALES="${1:-100000 1000000}"
DB=data/_bench.duckdb
# ★ 안정화: 대량+풀병렬 시 DuckDB 1.5.3 세그폴트(경쟁조건) 회피 → 메모리 한도·스레드 제한
STABLE="SET memory_limit='24GB'; SET threads TO 6;"

printf "%-12s %10s %10s %12s %12s %12s\n" "orders" "gen(s)" "settle(s)" "events" "ev/s" "agg"
for N in $SCALES; do
  rm -f "$DB"
  # 시드 규모 치환
  sed "s/__ORDERS__/$N/" bench/bench_seed.sql > /tmp/_bench_seed.sql

  # ① 생성 시간
  t0=$(date +%s.%N)
  (echo "$STABLE"; cat transform/01_schema.sql /tmp/_bench_seed.sql) \
    | docker run --rm -i -v "$PWD":/work "$IMG" /work/"$DB" >/dev/null 2>&1
  t1=$(date +%s.%N)

  # ② 정산 시간
  (echo "$STABLE"; cat transform/03_settlement.sql) \
    | docker run --rm -i -v "$PWD":/work "$IMG" /work/"$DB" >/dev/null 2>&1
  t2=$(date +%s.%N)

  # 행수 (CSV 모드로 깔끔히 파싱)
  EV=$(echo "SELECT COUNT(*) FROM settlement_event;" \
    | docker run --rm -i -v "$PWD":/work "$IMG" -csv -noheader /work/"$DB" 2>/dev/null | tr -dc '0-9')
  AG=$(echo "SELECT COUNT(*) FROM settlement_agg;" \
    | docker run --rm -i -v "$PWD":/work "$IMG" -csv -noheader /work/"$DB" 2>/dev/null | tr -dc '0-9')

  gen=$(echo "$t1 - $t0" | bc); set_=$(echo "$t2 - $t1" | bc)
  evs=$(echo "scale=0; $EV / $set_" | bc 2>/dev/null || echo "-")
  printf "%-12s %10.1f %10.1f %12s %12s %12s\n" "$N" "$gen" "$set_" "$EV" "$evs" "$AG"
done
rm -f "$DB"
