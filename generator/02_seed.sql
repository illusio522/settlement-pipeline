-- 1단계 시드 v2 — 복합결제(주: 카드·머니 / 보조: 포인트·쿠폰) + 부분취소
-- ★ 재현성(2종 비결정 모두 차단):
--   ① 가변 호출: 한 행서 random() 호출수가 분기마다 다르면 threads=1로도 안 잡힘 → partner_id·cancel은 결정적 hash로 배정(아래).
--   ② 고정 호출: 저장 테이블을 morsel 병렬 스캔하며 random() 쓰면 흔들림 → 시드 구간을 단일 스레드로 고정해 차단.
SET threads TO 1;
SELECT setseed(0.42);

-- 마스터
INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);

-- 결제수단별 정산 수수료(차등, as-of): 카드 2.5% 머니 1.5% 포인트/쿠폰 0%, 카드 6/1 인하
INSERT INTO method_fee VALUES
  (1, 0.0250, DATE '2020-01-01'),   -- CARD
  (2, 0.0150, DATE '2020-01-01'),   -- MONEY
  (3, 0.0000, DATE '2020-01-01'),   -- POINT
  (4, 0.0000, DATE '2020-01-01'),   -- COUPON
  (1, 0.0230, DATE '2026-06-01');   -- CARD 인하(6/1~) → as-of 검증

-- mid_map: 브랜드당 카드 MID 2개 + 머니 MID 1개 (주결제수단별 MID)
INSERT INTO mid_map VALUES
  (1001,1),(1003,1),(1002,2),(1004,2),   -- 카드
  (1101,1),(1102,2);                      -- 머니

-- 주문 + order_item (스큐)
CREATE OR REPLACE TEMP TABLE _orders AS
SELECT o AS order_no, (CASE WHEN random()<0.5 THEN 1 ELSE 2 END)::SMALLINT AS brand
FROM generate_series(1,100000) AS s(o);
INSERT INTO order_item
WITH lines AS (
  SELECT order_no, brand, l AS line_no FROM _orders CROSS JOIN generate_series(1,3) AS g3(l)
  WHERE l=1 OR (l=2 AND random()<0.5) OR (l=3 AND random()<0.25)
)
SELECT order_no, line_no::SMALLINT,
  -- partner_id: 결정적 해시(순수함수) 배정 — 입력 같으면 항상 같은 값이라 morsel 병렬·스레드 무관
  CASE WHEN brand=1 AND (hash(order_no||':'||line_no||':a')%100) < 70 THEN 1
       WHEN brand=1 THEN 2 + (hash(order_no||':'||line_no||':b')%48)::INT
       ELSE 51 + (hash(order_no||':'||line_no||':b')%49)::INT END,
  brand, (10000 + (random()*90000)::INT)::BIGINT
FROM lines;

-- payment 헤더: PAY 1건/주문 (amount=총액)
INSERT INTO payment
WITH ot AS (SELECT order_no, MAX(brand) AS brand, SUM(item_amount) AS total FROM order_item GROUP BY order_no)
SELECT order_no, order_no, brand, total, 1::SMALLINT, NULL::BIGINT,
       TIMESTAMP '2026-05-15' + to_seconds((random()*2160000)::BIGINT)
FROM ot;

-- payment_method: PAY 복합결제 분해
--   보조(포인트·쿠폰) 차감 후, 주결제수단을 카드단독/머니단독/카드+머니(동시)로 분배
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
    -- 20% 카드+머니 동시 / 60% 카드단독 / 20% 머니단독
    CASE WHEN r_split<0.20 THEN CAST(ROUND((amount-point_amt-coupon_amt)*0.6) AS BIGINT)
         WHEN r_split<0.80 THEN (amount-point_amt-coupon_amt)
         ELSE 0 END AS card_amt,
    (CASE WHEN brand=1 THEN (CASE WHEN r_mid<0.5 THEN 1001 ELSE 1003 END)
          ELSE (CASE WHEN r_mid<0.5 THEN 1002 ELSE 1004 END) END) AS card_mid,
    (CASE WHEN brand=1 THEN 1101 ELSE 1102 END) AS money_mid
  FROM mix
),
m2 AS (SELECT *, (main_total - card_amt) AS money_amt FROM m)   -- 나머지는 머니
SELECT txn_id*10+1, txn_id, order_no, 1::SMALLINT, card_mid,  card_amt,  1::SMALLINT, event_time FROM m2 WHERE card_amt>0
UNION ALL
SELECT txn_id*10+2, txn_id, order_no, 2::SMALLINT, money_mid, money_amt, 1::SMALLINT, event_time FROM m2 WHERE money_amt>0
UNION ALL
SELECT txn_id*10+3, txn_id, order_no, 3::SMALLINT, NULL::INTEGER, point_amt,  1::SMALLINT, event_time FROM m2 WHERE point_amt>0
UNION ALL
SELECT txn_id*10+4, txn_id, order_no, 4::SMALLINT, NULL::INTEGER, coupon_amt, 1::SMALLINT, event_time FROM m2 WHERE coupon_amt>0;

-- ===== 취소: 전체/부분 (파트너 정산용 라인 명세) =====
CREATE OR REPLACE TEMP TABLE _ord_lc AS
SELECT order_no, COUNT(*) AS lc, MAX(line_no) AS max_line FROM order_item GROUP BY order_no;
CREATE OR REPLACE TEMP TABLE _cxl AS
SELECT lc.order_no, lc.lc, lc.max_line,
       -- 결정적 해시: 취소 대상 선택(5%)·부분취소 여부(50%) 모두 순수함수로 → 재현성 보장
       CASE WHEN lc.lc>1 AND (hash(lc.order_no||':k')%100)<50 THEN 'partial' ELSE 'full' END AS kind
FROM _ord_lc lc WHERE (hash(lc.order_no||':sel')%100) < 5;
INSERT INTO cancel
SELECT 100000000 + x.order_no, oi.order_no, oi.line_no
FROM _cxl x JOIN order_item oi ON oi.order_no=x.order_no WHERE x.kind='full'
UNION ALL
SELECT 100000000 + x.order_no, x.order_no, x.max_line FROM _cxl x WHERE x.kind='partial';

-- payment CANCEL 헤더: amount=취소 라인 합
INSERT INTO payment
SELECT c.cancel_txn_id, c.order_no, MAX(oi.brand), SUM(oi.item_amount), 2::SMALLINT, c.order_no,
       TIMESTAMP '2026-06-02' + to_seconds(c.order_no % 600000)
FROM cancel c JOIN order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
GROUP BY c.cancel_txn_id, c.order_no;
