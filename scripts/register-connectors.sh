#!/usr/bin/env bash
# Debezium 커넥터 등록 — JSON의 자격증명 플레이스홀더를 infra/.env 값으로 치환해 POST.
#   JSON에 비밀번호를 두지 않으므로 수동 curl 대신 이 스크립트로 등록한다.
#   치환은 bash 파라미터 확장으로만 수행(외부 프로세스 미사용 → ps 노출 없음).
#   이미 등록된 커넥터는 건너뛴다. 설정을 바꿨으면 먼저 DELETE 후 재실행.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f infra/.env ] || { echo "infra/.env 없음 — infra/.env.example 를 복사해 만드세요"; exit 1; }
set -a; . infra/.env; set +a

CONNECT="${CONNECT_URL:-http://127.0.0.1:8083}"

for f in infra/debezium/pay-connector.json infra/debezium/order-connector.json; do
  name=$(basename "$f" .json)
  if curl -sf -o /dev/null "$CONNECT/connectors/$name"; then
    echo "· $name 이미 등록됨 (건너뜀)"
    continue
  fi
  echo "▶ $name 등록"
  while IFS= read -r line; do
    line=${line//__POSTGRES_USER__/$POSTGRES_USER}
    line=${line//__POSTGRES_PASSWORD__/$POSTGRES_PASSWORD}
    printf '%s\n' "$line"
  done < "$f" \
  | curl -sS -X POST -H 'Content-Type: application/json' --data-binary @- "$CONNECT/connectors" \
       -o /dev/null -w "  HTTP %{http_code}\n"
done

echo "▶ 커넥터 상태"
curl -sS "$CONNECT/connectors?expand=status" \
  | tr ',' '\n' | grep -E '"name"|"state"' | head -20
