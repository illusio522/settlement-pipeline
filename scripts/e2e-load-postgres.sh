#!/usr/bin/env bash
# 2-5 작업1: 1단계 canonical(data/settlement.duckdb) → Postgres 현실형 스키마로 적재.
#   - payment_method(단일,method 1~4) → 수단별 4원장(card/money/point/coupon)으로 분리
#   - order_item: product_id/qty/unit_price 파생 채움 (현실 컬럼)
#   - 나머지 현실 컬럼(currency·status·audit 등)은 Postgres DEFAULT
# 단일 출처 원칙: 두 곳서 따로 생성하면 hash 함수가 달라 데이터 어긋남 → 1단계를 정답지로 export.
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=settlement-duckdb:1.5.3
DB=/work/data/settlement.duckdb
OUT=data/e2e
mkdir -p "$OUT"

echo "▶ 1) 1단계 canonical → CSV export (payment_method 4원장 분리)"
cat <<SQL | docker run --rm -i -v "$PWD":/work "$IMG" "$DB"
COPY (SELECT txn_id, order_no, brand, amount, type, orig_txn_id, event_time FROM payment)
  TO '/work/$OUT/payment.csv' (FORMAT CSV, HEADER);
COPY (SELECT pm_id, txn_id, order_no, payments_mid, amount, type, event_time FROM payment_method WHERE method=1)
  TO '/work/$OUT/payment_card.csv' (FORMAT CSV, HEADER);
COPY (SELECT pm_id, txn_id, order_no, payments_mid, amount, type, event_time FROM payment_method WHERE method=2)
  TO '/work/$OUT/payment_money.csv' (FORMAT CSV, HEADER);
COPY (SELECT pm_id, txn_id, order_no, amount, type, event_time FROM payment_method WHERE method=3)
  TO '/work/$OUT/payment_point.csv' (FORMAT CSV, HEADER);
COPY (SELECT pm_id, txn_id, order_no, amount, type, event_time FROM payment_method WHERE method=4)
  TO '/work/$OUT/payment_coupon.csv' (FORMAT CSV, HEADER);
COPY (SELECT order_no, line_no, order_no AS product_id, 1 AS qty, item_amount AS unit_price, item_amount, partner_id, brand FROM order_item)
  TO '/work/$OUT/order_item.csv' (FORMAT CSV, HEADER);
COPY (SELECT cancel_txn_id, order_no, line_no FROM cancel)
  TO '/work/$OUT/cancel.csv' (FORMAT CSV, HEADER);
SQL

echo "▶ 2) Postgres 소스 테이블 TRUNCATE (깨끗한 적재)"
docker exec sp-postgres psql -U settle -d paydb -q -c \
  "TRUNCATE payment, payment_card, payment_money, payment_point, payment_coupon;"
docker exec sp-postgres psql -U settle -d orderdb -q -c \
  "TRUNCATE order_item, cancel;"

echo "▶ 3) CSV → Postgres COPY (현실 컬럼은 DEFAULT)"
load() { # $1=db $2=table $3=columns $4=csv
  cat "$OUT/$4" | docker exec -i sp-postgres psql -U settle -d "$1" -q -c \
    "\copy $2($3) FROM STDIN WITH (FORMAT csv, HEADER true)"
}
load paydb   payment        "txn_id,order_no,brand,amount,type,orig_txn_id,event_time"      payment.csv
load paydb   payment_card   "pm_id,txn_id,order_no,payments_mid,amount,type,event_time"     payment_card.csv
load paydb   payment_money  "pm_id,txn_id,order_no,payments_mid,amount,type,event_time"     payment_money.csv
load paydb   payment_point  "pm_id,txn_id,order_no,amount,type,event_time"                  payment_point.csv
load paydb   payment_coupon "pm_id,txn_id,order_no,amount,type,event_time"                  payment_coupon.csv
load orderdb order_item     "order_no,line_no,product_id,qty,unit_price,item_amount,partner_id,brand" order_item.csv
load orderdb cancel         "cancel_txn_id,order_no,line_no"                                cancel.csv

echo "▶ 4) 적재 건수 확인"
docker exec sp-postgres psql -U settle -d paydb -q -c \
  "SELECT 'payment' t,COUNT(*) FROM payment UNION ALL SELECT 'card',COUNT(*) FROM payment_card UNION ALL SELECT 'money',COUNT(*) FROM payment_money UNION ALL SELECT 'point',COUNT(*) FROM payment_point UNION ALL SELECT 'coupon',COUNT(*) FROM payment_coupon ORDER BY 1;"
docker exec sp-postgres psql -U settle -d orderdb -q -c \
  "SELECT 'order_item' t,COUNT(*) FROM order_item UNION ALL SELECT 'cancel',COUNT(*) FROM cancel ORDER BY 1;"
echo "✅ Postgres 적재 완료"
