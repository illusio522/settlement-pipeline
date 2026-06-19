#!/bin/bash
# 컨슈머 공정 측정: concurrency·배분전략을 바꿔가며 랜딩 적재 시간 실측.
#  공정 원칙: ①동일 max-poll(2000, application.yml 고정) ②동일 입력(오프셋 earliest 리셋→같은 메시지 재소비)
#            ③종료 판정 = 랜딩 줄 수 plateau(더 안 늘면 완료), 마지막 증가시각 기준(감지지연 제외)
#  각 런: 그룹 비움 대기 → 오프셋 리셋 → 랜딩 truncate → 앱 기동(옵션 주입) → plateau까지 폴링
#         → 일꾼별 파티션 배정 스냅샷 → 앱 종료(graceful flush) → 최종 줄 수 확인
set -u
cd "$(dirname "$0")/.." || exit 1
ROOT="$(pwd)"
JAR="$ROOT/normalize/build/libs/settlement-normalize-0.1.0.jar"
LAND="$ROOT/data/landing"
GROUP="normalize-sink"
KAFKA="docker exec sp-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092"
TOPICS="pay.public.payment pay.public.payment_card pay.public.payment_money pay.public.payment_point pay.public.payment_coupon order.public.order_item order.public.cancel"
FLOOR=550000        # 이 줄 수 넘어야 plateau 인정(조기 오판 방지)
SNAP_AT=5000        # 이 줄 수 넘으면 일꾼 배정 스냅샷(소비 중반, 그룹 활성 보장)
PLATEAU=4           # 줄 수 무변화 N초 = 완료
POLL=0.5
RESULT="$ROOT/data/consumer-bench-result.txt"

now()  { python3 -c 'import time;print(f"{time.time():.3f}")'; }
lines(){ wc -l "$LAND"/*.ndjson 2>/dev/null | tail -1 | awk '{print $1+0}'; }
gef()  { awk "BEGIN{exit !($1>=$2)}"; }   # $1>=$2 면 true(0)

wait_empty() {
  for _ in $(seq 1 60); do
    s=$($KAFKA --describe --group "$GROUP" --state 2>/dev/null | grep -iw "$GROUP" | awk '{print $(NF-1)}')
    [ "$s" = "Empty" ] || [ "$s" = "Dead" ] && return 0
    sleep 1
  done
}

run_case() {
  local label="$1" conc="$2" strat="$3"
  echo "================= $label (concurrency=$conc, strategy=${strat##*.}) ================="
  wait_empty
  echo "[reset] 오프셋 earliest 리셋"
  $KAFKA --group "$GROUP" --reset-offsets --all-topics --to-earliest --execute >/dev/null 2>&1
  echo "[truncate] 랜딩 비우기(샤드 포함 전부)"
  rm -f "$LAND"/*.ndjson
  local t_launch t_first="" last=0 last_change
  t_launch=$(now); last_change=$(now)
  echo "[launch] java -jar (conc=$conc, ${strat##*.})"
  ( cd "$ROOT/normalize" && exec java -jar "$JAR" \
      --spring.kafka.listener.concurrency="$conc" \
      --spring.kafka.consumer.properties.partition.assignment.strategy="$strat" \
      --logging.level.com.settlement=WARN ) > "/tmp/cbench-$label.log" 2>&1 &
  local pid=$!
  local members_snap=""
  while kill -0 "$pid" 2>/dev/null; do
    sleep "$POLL"
    local tot; tot=$(lines)
    [ -z "$t_first" ] && [ "$tot" -gt 0 ] && t_first=$(now)
    if [ "$tot" -gt "$last" ]; then last=$tot; last_change=$(now); fi
    local nowt gap; nowt=$(now); gap=$(awk "BEGIN{print $nowt-$last_change}")
    # 소비 중반(그룹 활성)에 일꾼 배정 스냅샷 1회
    if [ -z "$members_snap" ] && gef "$tot" "$SNAP_AT"; then
      members_snap=$($KAFKA --describe --group "$GROUP" --members 2>/dev/null)
    fi
    if gef "$tot" "$FLOOR" && gef "$gap" "$PLATEAU"; then break; fi
    local el; el=$(awk "BEGIN{print $nowt-$t_launch}")
    if gef "$el" 600; then echo "[timeout] 600s"; break; fi
  done
  local work total; work=$(awk "BEGIN{printf \"%.2f\", $last_change-${t_first:-$t_launch}}")
  total=$(awk "BEGIN{printf \"%.2f\", $last_change-$t_launch}")
  echo "[shutdown] graceful (최종 flush)"
  kill "$pid" 2>/dev/null; sleep 4
  local final; final=$(lines)
  # 일꾼별 파티션 배정 집계
  local nmem nwork
  nmem=$(echo "$members_snap" | grep -c "$GROUP" 2>/dev/null)
  nwork=$(echo "$members_snap" | grep "$GROUP" | awk '{if($NF+0>0)c++} END{print c+0}')
  {
    echo "## $label  conc=$conc  strategy=${strat##*.}"
    echo "  소비완료(plateau): ${last}줄 / 최종(flush후): ${final}줄"
    echo "  적재시간(work, 첫줄→plateau): ${work}s"
    echo "  총시간(launch→plateau):        ${total}s"
    echo "  일꾼: 멤버 ${nmem}명 중 실제 파티션 보유 ${nwork}명"
    echo "  --- 일꾼별 파티션 ---"
    echo "$members_snap" | grep "$GROUP" | awk '{printf "    %-30s parts=%s\n",$4,$NF}'
    echo ""
  } | tee -a "$RESULT"
  wait_empty
}

: > "$RESULT"
echo "측정 시작 $(date)" | tee -a "$RESULT"
run_case "M1-single"   1 "org.apache.kafka.clients.consumer.RangeAssignor"
run_case "M2-range8"   8 "org.apache.kafka.clients.consumer.RangeAssignor"
run_case "M3-coop8"    8 "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
echo "==== 측정 종료 ====" | tee -a "$RESULT"
cat "$RESULT"
