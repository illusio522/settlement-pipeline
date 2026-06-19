-- 1단계 정산 v3: 조인→배분(라인)→교차배분(라인×결제수단)→FREEZE(차등수수료)→집계→대사
-- 정산은 random 미사용·멱등(검증됨) → 시드 구간서 내린 threads 원복해 병렬 처리
RESET threads;
TRUNCATE settlement_event; TRUNCATE settlement_agg; TRUNCATE recon_result;
TRUNCATE pg_settlement_file; TRUNCATE recon_card;

-- (a) 청구 라인: PAY=전 라인, CANCEL=취소 라인만
CREATE OR REPLACE TEMP TABLE _pay_line AS
SELECT p.txn_id, p.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount AS pay_amount
FROM payment p JOIN order_item oi ON oi.order_no=p.order_no WHERE p.type=1
UNION ALL
SELECT p.txn_id, c.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount
FROM payment p JOIN cancel c ON c.cancel_txn_id=p.txn_id
JOIN order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no WHERE p.type=2;

-- (b) 라인 배분 (윈도우 분리)
CREATE OR REPLACE TEMP TABLE _alloc_raw AS
SELECT *, CAST(event_time AS DATE) AS settlement_date,
       CAST(ROUND(pay_amount * item_amount::DOUBLE / SUM(item_amount) OVER (PARTITION BY txn_id)) AS BIGINT) AS raw_alloc
FROM _pay_line;
CREATE OR REPLACE TEMP TABLE _alloc AS
SELECT *, CASE WHEN line_no = MAX(line_no) OVER (PARTITION BY txn_id)
       THEN pay_amount - (SUM(raw_alloc) OVER (PARTITION BY txn_id) - raw_alloc) ELSE raw_alloc END AS allocated_amount
FROM _alloc_raw;

-- (c) 주문별 결제수단 비율 (PAY 기준) — CANCEL 라인도 원결제 구성 따름
CREATE OR REPLACE TEMP TABLE _order_method AS
SELECT order_no, method, payments_mid, amount AS m_amount,
       SUM(amount) OVER (PARTITION BY order_no) AS order_total
FROM payment_method WHERE type=1;

-- (d) 교차 배분 (라인 × 결제수단), 라인 내 잔액보정
CREATE OR REPLACE TEMP TABLE _detail_raw AS
SELECT a.txn_id, a.order_no, a.line_no, a.partner_id, a.brand, a.type, a.event_time, a.settlement_date,
       a.allocated_amount AS line_alloc, om.method, om.payments_mid,
       CAST(ROUND(a.allocated_amount * om.m_amount::DOUBLE / om.order_total) AS BIGINT) AS raw_m
FROM _alloc a JOIN _order_method om ON om.order_no = a.order_no;
CREATE OR REPLACE TEMP TABLE _detail AS
SELECT *, CASE WHEN method = MAX(method) OVER (PARTITION BY txn_id, line_no)
       THEN line_alloc - (SUM(raw_m) OVER (PARTITION BY txn_id, line_no) - raw_m) ELSE raw_m END AS method_alloc
FROM _detail_raw;

-- (e) FREEZE: 결제수단별 as-of 수수료 → settlement_event
INSERT INTO settlement_event
SELECT d.brand||':'||d.txn_id||':'||d.line_no||':'||d.method, d.txn_id, d.order_no, d.line_no,
       d.partner_id, d.brand, d.type, d.method, d.payments_mid,
       d.method_alloc, f.fee_rate, CAST(ROUND(d.method_alloc*f.fee_rate) AS BIGINT) AS fee,
       d.method_alloc - CAST(ROUND(d.method_alloc*f.fee_rate) AS BIGINT) AS net_amount, d.event_time, d.settlement_date
FROM _detail d
ASOF JOIN method_fee f ON f.method = d.method AND d.settlement_date >= f.effective_from;

-- 집계: 결제수단별 명세 (정산일×파트너×브랜드×결제수단)
INSERT INTO settlement_agg
SELECT settlement_date, partner_id, brand, method,
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0),
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0),
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0),
  COALESCE(SUM(fee) FILTER (WHERE type=1),0),
  (COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0))-COALESCE(SUM(fee) FILTER (WHERE type=1),0),
  COUNT(*)
FROM settlement_event GROUP BY settlement_date, partner_id, brand, method;

-- 대사1: 파트너 보존 (주문총액 = 집계 gross 합)
INSERT INTO recon_result
WITH src AS (SELECT CAST(event_time AS DATE) sd, brand, COALESCE(SUM(amount) FILTER (WHERE type=1),0) src_pay FROM payment GROUP BY 1,2),
     agg AS (SELECT settlement_date sd, brand, SUM(gross) agg_pay FROM settlement_agg GROUP BY 1,2)
SELECT COALESCE(s.sd,a.sd), COALESCE(s.brand,a.brand), COALESCE(s.src_pay,0), COALESCE(a.agg_pay,0),
       COALESCE(s.src_pay,0)-COALESCE(a.agg_pay,0), (CASE WHEN COALESCE(s.src_pay,0)=COALESCE(a.agg_pay,0) THEN 0 ELSE 1 END)::SMALLINT
FROM src s FULL OUTER JOIN agg a ON s.sd=a.sd AND s.brand=a.brand;

-- PG파일 생성 + 대사2: 주결제수단 MID 대사
INSERT INTO pg_settlement_file
SELECT CAST(event_time AS DATE), payments_mid, SUM(amount) FROM payment_method WHERE type=1 AND payments_mid IS NOT NULL GROUP BY 1,2;
INSERT INTO recon_card
WITH pm AS (SELECT CAST(event_time AS DATE) sd, payments_mid, SUM(amount) card_total FROM payment_method WHERE type=1 AND payments_mid IS NOT NULL GROUP BY 1,2)
SELECT COALESCE(pm.sd,f.settlement_date), COALESCE(pm.payments_mid,f.payments_mid), COALESCE(pm.card_total,0), COALESCE(f.pg_amount,0),
       COALESCE(pm.card_total,0)-COALESCE(f.pg_amount,0), (CASE WHEN COALESCE(pm.card_total,0)=COALESCE(f.pg_amount,0) THEN 0 ELSE 1 END)::SMALLINT
FROM pm FULL OUTER JOIN pg_settlement_file f ON pm.sd=f.settlement_date AND pm.payments_mid=f.payments_mid;
