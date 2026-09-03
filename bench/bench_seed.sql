-- 성능 벤치용 시드 (규모 파라미터 __ORDERS__). 02_seed의 복합결제 구조 유지.
--   재현성 불필요(성능 측정) → 멀티스레드 random 허용(SET threads TO 1 제거).
--   파트너는 100 고정 → 주문 늘수록 파트너1 스큐 밀도 ↑ (스큐 실험).
SELECT setseed(0.42);

INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'mall-a-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);

INSERT INTO method_fee VALUES
  (1,0.0250,DATE '2020-01-01'),(2,0.0150,DATE '2020-01-01'),
  (3,0.0000,DATE '2020-01-01'),(4,0.0000,DATE '2020-01-01'),(1,0.0230,DATE '2026-06-01');
INSERT INTO mid_map VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2);

CREATE OR REPLACE TEMP TABLE _orders AS
SELECT o AS order_no, (CASE WHEN random()<0.5 THEN 1 ELSE 2 END)::SMALLINT AS brand
FROM generate_series(1,__ORDERS__) AS s(o);

INSERT INTO order_item
WITH lines AS (
  SELECT order_no, brand, l AS line_no FROM _orders CROSS JOIN generate_series(1,3) AS g3(l)
  WHERE l=1 OR (l=2 AND random()<0.5) OR (l=3 AND random()<0.25)
)
SELECT order_no, line_no::SMALLINT,
  CASE WHEN brand=1 AND random()<0.7 THEN 1
       WHEN brand=1 THEN 2 + (random()*48)::INT
       ELSE 51 + (random()*49)::INT END,
  brand, (10000 + (random()*90000)::INT)::BIGINT
FROM lines;

INSERT INTO payment
WITH ot AS (SELECT order_no, MAX(brand) AS brand, SUM(item_amount) AS total FROM order_item GROUP BY order_no)
SELECT order_no, order_no, brand, total, 1::SMALLINT, NULL::BIGINT,
       TIMESTAMP '2026-05-15' + to_seconds((random()*2160000)::BIGINT)
FROM ot;

INSERT INTO payment_method
WITH p AS (SELECT txn_id, order_no, brand, amount, event_time FROM payment WHERE type=1),
mix AS (
  SELECT *, random() AS r_split, random() AS r_mid,
    CASE WHEN random()<0.30 THEN CAST(ROUND(amount*0.15) AS BIGINT) ELSE 0 END AS point_amt,
    CASE WHEN random()<0.15 THEN CAST(ROUND(amount*0.10) AS BIGINT) ELSE 0 END AS coupon_amt
  FROM p
),
m AS (
  SELECT *, (amount - point_amt - coupon_amt) AS main_total,
    CASE WHEN r_split<0.20 THEN CAST(ROUND((amount-point_amt-coupon_amt)*0.6) AS BIGINT)
         WHEN r_split<0.80 THEN (amount-point_amt-coupon_amt)
         ELSE 0 END AS card_amt,
    (CASE WHEN brand=1 THEN (CASE WHEN r_mid<0.5 THEN 1001 ELSE 1003 END)
          ELSE (CASE WHEN r_mid<0.5 THEN 1002 ELSE 1004 END) END) AS card_mid,
    (CASE WHEN brand=1 THEN 1101 ELSE 1102 END) AS money_mid
  FROM mix
),
m2 AS (SELECT *, (main_total - card_amt) AS money_amt FROM m)
SELECT txn_id*10+1, txn_id, order_no, 1::SMALLINT, card_mid,  card_amt,  1::SMALLINT, event_time FROM m2 WHERE card_amt>0
UNION ALL
SELECT txn_id*10+2, txn_id, order_no, 2::SMALLINT, money_mid, money_amt, 1::SMALLINT, event_time FROM m2 WHERE money_amt>0
UNION ALL
SELECT txn_id*10+3, txn_id, order_no, 3::SMALLINT, NULL::INTEGER, point_amt,  1::SMALLINT, event_time FROM m2 WHERE point_amt>0
UNION ALL
SELECT txn_id*10+4, txn_id, order_no, 4::SMALLINT, NULL::INTEGER, coupon_amt, 1::SMALLINT, event_time FROM m2 WHERE coupon_amt>0;

CREATE OR REPLACE TEMP TABLE _ord_lc AS
SELECT order_no, COUNT(*) AS lc, MAX(line_no) AS max_line FROM order_item GROUP BY order_no;
CREATE OR REPLACE TEMP TABLE _cxl AS
SELECT lc.order_no, lc.lc, lc.max_line,
       CASE WHEN lc.lc>1 AND random()<0.5 THEN 'partial' ELSE 'full' END AS kind
FROM _ord_lc lc WHERE random()<0.05;
INSERT INTO cancel
SELECT 100000000 + x.order_no, oi.order_no, oi.line_no
FROM _cxl x JOIN order_item oi ON oi.order_no=x.order_no WHERE x.kind='full'
UNION ALL
SELECT 100000000 + x.order_no, x.order_no, x.max_line FROM _cxl x WHERE x.kind='partial';

INSERT INTO payment
SELECT c.cancel_txn_id, c.order_no, MAX(oi.brand), SUM(oi.item_amount), 2::SMALLINT, c.order_no,
       TIMESTAMP '2026-06-02' + to_seconds(c.order_no % 600000)
FROM cancel c JOIN order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
GROUP BY c.cancel_txn_id, c.order_no;
