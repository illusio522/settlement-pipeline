-- 7-2c Gold: silver → 정산 (03_settlement.sql 의 Spark SQL 포팅)
--   로직 동일(조인→배분→교차배분→FREEZE→집계→대사), 문법만 포팅:
--   · TEMP TABLE → TEMPORARY VIEW  · ASOF JOIN → row_number() 최신 1건
--   · x::DOUBLE → CAST  · a||b → concat  · epoch/round 는 동일 의미 함수
--   실행: bash scripts/spark-sql.sh transform/spark_gold.sql
CREATE NAMESPACE IF NOT EXISTS nessie.gold;

DROP TABLE IF EXISTS nessie.gold.settlement_event;
DROP TABLE IF EXISTS nessie.gold.settlement_agg;
DROP TABLE IF EXISTS nessie.gold.pg_settlement_file;
DROP TABLE IF EXISTS nessie.gold.recon_result;
DROP TABLE IF EXISTS nessie.gold.recon_card;

-- (a) 청구 라인: PAY=전 라인, CANCEL=취소 라인만
CREATE OR REPLACE TEMPORARY VIEW _pay_line AS
SELECT p.txn_id, p.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount AS pay_amount
FROM nessie.silver.payment p JOIN nessie.silver.order_item oi ON oi.order_no = p.order_no WHERE p.type = 1
UNION ALL
SELECT p.txn_id, c.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount
FROM nessie.silver.payment p JOIN nessie.silver.cancel c ON c.cancel_txn_id = p.txn_id
JOIN nessie.silver.order_item oi ON oi.order_no = c.order_no AND oi.line_no = c.line_no WHERE p.type = 2;

-- (b) 라인 배분 (윈도우 분리, 마지막 라인 잔액보정)
CREATE OR REPLACE TEMPORARY VIEW _alloc_raw AS
SELECT *, CAST(event_time AS DATE) AS settlement_date,
       CAST(ROUND(pay_amount * CAST(item_amount AS DOUBLE) / SUM(item_amount) OVER (PARTITION BY txn_id)) AS BIGINT) AS raw_alloc
FROM _pay_line;
CREATE OR REPLACE TEMPORARY VIEW _alloc AS
SELECT *, CASE WHEN line_no = MAX(line_no) OVER (PARTITION BY txn_id)
       THEN pay_amount - (SUM(raw_alloc) OVER (PARTITION BY txn_id) - raw_alloc) ELSE raw_alloc END AS allocated_amount
FROM _alloc_raw;

-- (c) 주문별 결제수단 비율 (PAY 기준)
CREATE OR REPLACE TEMPORARY VIEW _order_method AS
SELECT order_no, method, payments_mid, amount AS m_amount,
       SUM(amount) OVER (PARTITION BY order_no) AS order_total
FROM nessie.silver.payment_method WHERE type = 1;

-- (d) 교차 배분 (라인 × 결제수단), 라인 내 잔액보정
CREATE OR REPLACE TEMPORARY VIEW _detail_raw AS
SELECT a.txn_id, a.order_no, a.line_no, a.partner_id, a.brand, a.type, a.event_time, a.settlement_date,
       a.allocated_amount AS line_alloc, om.method, om.payments_mid,
       CAST(ROUND(a.allocated_amount * CAST(om.m_amount AS DOUBLE) / om.order_total) AS BIGINT) AS raw_m
FROM _alloc a JOIN _order_method om ON om.order_no = a.order_no;
CREATE OR REPLACE TEMPORARY VIEW _detail AS
SELECT *, CASE WHEN method = MAX(method) OVER (PARTITION BY txn_id, line_no)
       THEN line_alloc - (SUM(raw_m) OVER (PARTITION BY txn_id, line_no) - raw_m) ELSE raw_m END AS method_alloc
FROM _detail_raw;

-- (e) FREEZE: as-of 수수료 — ASOF JOIN 대체 = 조건조인 + effective_from 최신 1건(row_number)
CREATE TABLE nessie.gold.settlement_event USING iceberg AS
SELECT concat(CAST(brand AS STRING), ':', CAST(txn_id AS STRING), ':', CAST(line_no AS STRING), ':', CAST(method AS STRING)) AS event_id,
       txn_id, order_no, line_no, partner_id, brand, type, method, payments_mid,
       method_alloc AS allocated_amount, fee_rate,
       CAST(ROUND(method_alloc * fee_rate) AS BIGINT) AS fee,
       method_alloc - CAST(ROUND(method_alloc * fee_rate) AS BIGINT) AS net_amount,
       event_time, settlement_date
FROM (
  SELECT d.*, f.fee_rate,
         row_number() OVER (PARTITION BY d.txn_id, d.line_no, d.method ORDER BY f.effective_from DESC) AS frn
  FROM _detail d
  JOIN nessie.silver.method_fee f ON f.method = d.method AND d.settlement_date >= f.effective_from
) WHERE frn = 1;

-- 집계 (정산일×파트너×브랜드×결제수단)
CREATE TABLE nessie.gold.settlement_agg USING iceberg AS
SELECT settlement_date, partner_id, brand, method,
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0) AS gross,
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0) AS cancel,
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0) AS net,
  COALESCE(SUM(fee) FILTER (WHERE type=1),0) AS fee,
  (COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0))-COALESCE(SUM(fee) FILTER (WHERE type=1),0) AS payout,
  COUNT(*) AS txn_count
FROM nessie.gold.settlement_event GROUP BY settlement_date, partner_id, brand, method;

-- 대사1: 돈 보존 (주문총액 = 집계 gross)
CREATE TABLE nessie.gold.recon_result USING iceberg AS
WITH src AS (SELECT CAST(event_time AS DATE) sd, brand, COALESCE(SUM(amount) FILTER (WHERE type=1),0) src_pay
             FROM nessie.silver.payment GROUP BY 1,2),
     agg AS (SELECT settlement_date sd, brand, SUM(gross) agg_pay FROM nessie.gold.settlement_agg GROUP BY 1,2)
SELECT COALESCE(s.sd,a.sd) AS sd, COALESCE(s.brand,a.brand) AS brand,
       COALESCE(s.src_pay,0) AS src_pay, COALESCE(a.agg_pay,0) AS agg_pay,
       COALESCE(s.src_pay,0)-COALESCE(a.agg_pay,0) AS diff,
       CAST(CASE WHEN COALESCE(s.src_pay,0)=COALESCE(a.agg_pay,0) THEN 0 ELSE 1 END AS SMALLINT) AS mismatch
FROM src s FULL OUTER JOIN agg a ON s.sd=a.sd AND s.brand=a.brand;

-- PG파일 + 대사2: 주결제수단 MID
CREATE TABLE nessie.gold.pg_settlement_file USING iceberg AS
SELECT CAST(event_time AS DATE) AS settlement_date, payments_mid, SUM(amount) AS pg_amount
FROM nessie.silver.payment_method WHERE type=1 AND payments_mid IS NOT NULL GROUP BY 1,2;

CREATE TABLE nessie.gold.recon_card USING iceberg AS
WITH pm AS (SELECT CAST(event_time AS DATE) sd, payments_mid, SUM(amount) card_total
            FROM nessie.silver.payment_method WHERE type=1 AND payments_mid IS NOT NULL GROUP BY 1,2)
SELECT COALESCE(pm.sd,f.settlement_date) AS sd, COALESCE(pm.payments_mid,f.payments_mid) AS payments_mid,
       COALESCE(pm.card_total,0) AS card_total, COALESCE(f.pg_amount,0) AS pg_amount,
       COALESCE(pm.card_total,0)-COALESCE(f.pg_amount,0) AS diff,
       CAST(CASE WHEN COALESCE(pm.card_total,0)=COALESCE(f.pg_amount,0) THEN 0 ELSE 1 END AS SMALLINT) AS mismatch
FROM pm FULL OUTER JOIN nessie.gold.pg_settlement_file f
  ON pm.sd=f.settlement_date AND pm.payments_mid=f.payments_mid;

-- 검증 출력
SELECT '=== gold counts ===' AS marker;
SELECT 'settlement_event' AS tbl, COUNT(*) AS cnt FROM nessie.gold.settlement_event
UNION ALL SELECT 'settlement_agg', COUNT(*) FROM nessie.gold.settlement_agg
ORDER BY tbl;
SELECT '=== recon (mismatch 0,0 기대) ===' AS marker;
SELECT (SELECT SUM(mismatch) FROM nessie.gold.recon_result) AS recon_money,
       (SELECT SUM(mismatch) FROM nessie.gold.recon_card) AS recon_mid;

-- 체크섬 대조용 export: DuckDB가 읽어 정답지와 값 비교 (엔진 간 hash 상이 → 같은 엔진에서 대조)
INSERT OVERWRITE DIRECTORY '/work/data/spark_export/settlement_agg' USING parquet
SELECT * FROM nessie.gold.settlement_agg;
