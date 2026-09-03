-- 7-2b Silver: bronze → dedup canonical 4테이블 + 마스터 3테이블 (Iceberg)
--   dedup = key별 _seq(LSN) 최대 & _op<>'d' (읽기 시점 멱등 — B방식)
--   event_time: epoch밀리초 → TIMESTAMP (DuckDB epoch_ms 대응 = timestamp_millis)
--   실행: bash scripts/spark-sql.sh transform/spark_silver.sql
CREATE NAMESPACE IF NOT EXISTS nessie.silver;

DROP TABLE IF EXISTS nessie.silver.payment;
DROP TABLE IF EXISTS nessie.silver.payment_method;
DROP TABLE IF EXISTS nessie.silver.order_item;
DROP TABLE IF EXISTS nessie.silver.cancel;
DROP TABLE IF EXISTS nessie.silver.partner;
DROP TABLE IF EXISTS nessie.silver.method_fee;
DROP TABLE IF EXISTS nessie.silver.mid_map;

-- ===== 마스터 (1단계 02_seed와 동일 값) =====
CREATE TABLE nessie.silver.partner USING iceberg AS
SELECT g AS partner_id,
       CASE WHEN g = 1 THEN 'mall-a-main' ELSE concat('partner-', CAST(g AS STRING)) END AS name,
       CASE WHEN g <= 50 THEN 1 ELSE 2 END AS brand
FROM (SELECT explode(sequence(1, 100)) AS g);

CREATE TABLE nessie.silver.method_fee USING iceberg AS
SELECT method, CAST(fee_rate AS DECIMAL(5,4)) AS fee_rate, effective_from
FROM VALUES
  (1, 0.0250, DATE '2020-01-01'),
  (2, 0.0150, DATE '2020-01-01'),
  (3, 0.0000, DATE '2020-01-01'),
  (4, 0.0000, DATE '2020-01-01'),
  (1, 0.0230, DATE '2026-06-01')
AS t(method, fee_rate, effective_from);

CREATE TABLE nessie.silver.mid_map USING iceberg AS
SELECT * FROM VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2)
AS t(payments_mid, brand);

-- ===== canonical (bronze dedup) =====
CREATE TABLE nessie.silver.payment USING iceberg AS
SELECT txn_id, order_no, brand, amount, type, orig_txn_id, timestamp_millis(event_time) AS event_time
FROM (SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) AS rn
      FROM nessie.bronze.payment)
WHERE rn = 1 AND _op <> 'd';

CREATE TABLE nessie.silver.payment_method USING iceberg AS
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, timestamp_millis(event_time) AS event_time
FROM (SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) AS rn
      FROM nessie.bronze.payment_method)
WHERE rn = 1 AND _op <> 'd';

CREATE TABLE nessie.silver.order_item USING iceberg AS
SELECT order_no, line_no, partner_id, brand, item_amount
FROM (SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) AS rn
      FROM nessie.bronze.order_item)
WHERE rn = 1 AND _op <> 'd';

CREATE TABLE nessie.silver.cancel USING iceberg AS
SELECT cancel_txn_id, order_no, line_no
FROM (SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) AS rn
      FROM nessie.bronze.cancel)
WHERE rn = 1 AND _op <> 'd';

-- 검증: canonical 건수 + 고아(수단합≠헤더 / 헤더없는 수단)
SELECT '=== silver counts ===' AS marker;
SELECT 'payment' AS tbl, COUNT(*) AS cnt FROM nessie.silver.payment
UNION ALL SELECT 'payment_method', COUNT(*) FROM nessie.silver.payment_method
UNION ALL SELECT 'order_item', COUNT(*) FROM nessie.silver.order_item
UNION ALL SELECT 'cancel', COUNT(*) FROM nessie.silver.cancel
UNION ALL SELECT 'partner', COUNT(*) FROM nessie.silver.partner
UNION ALL SELECT 'method_fee', COUNT(*) FROM nessie.silver.method_fee
UNION ALL SELECT 'mid_map', COUNT(*) FROM nessie.silver.mid_map
ORDER BY tbl;

SELECT '=== orphan check (0,0 기대) ===' AS marker;
SELECT
  (SELECT COUNT(*) FROM nessie.silver.payment_method pm
     LEFT JOIN nessie.silver.payment p ON p.txn_id = pm.txn_id WHERE p.txn_id IS NULL) AS pm_no_header,
  (SELECT COUNT(*) FROM nessie.silver.cancel c
     LEFT JOIN nessie.silver.order_item oi ON oi.order_no = c.order_no AND oi.line_no = c.line_no
     WHERE oi.order_no IS NULL) AS cancel_no_line;
