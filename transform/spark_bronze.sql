-- 7-2a Bronze: 랜딩 NDJSON 샤드 → Iceberg bronze (원본 그대로, _op·_seq 보존)
--   메달리온 1층 — 손대지 않은 CDC 원본을 카탈로그 관리 테이블로.
--   실행: bash scripts/spark-sql.sh transform/spark_bronze.sql
CREATE NAMESPACE IF NOT EXISTS nessie.bronze;

-- 랜딩 샤드를 JSON 뷰로 (글롭 = 파티션 샤드 전부)
CREATE TEMPORARY VIEW v_payment        USING json OPTIONS (path '/work/data/landing/payment.p*.ndjson');
CREATE TEMPORARY VIEW v_payment_method USING json OPTIONS (path '/work/data/landing/payment_method.p*.ndjson');
CREATE TEMPORARY VIEW v_order_item     USING json OPTIONS (path '/work/data/landing/order_item.p*.ndjson');
CREATE TEMPORARY VIEW v_cancel         USING json OPTIONS (path '/work/data/landing/cancel.p*.ndjson');

-- 개발 재실행 멱등: 만들었다 다시 만들 수 있게 drop (운영이면 append만)
DROP TABLE IF EXISTS nessie.bronze.payment;
DROP TABLE IF EXISTS nessie.bronze.payment_method;
DROP TABLE IF EXISTS nessie.bronze.order_item;
DROP TABLE IF EXISTS nessie.bronze.cancel;

CREATE TABLE nessie.bronze.payment        USING iceberg AS SELECT * FROM v_payment;
CREATE TABLE nessie.bronze.payment_method USING iceberg AS SELECT * FROM v_payment_method;
CREATE TABLE nessie.bronze.order_item     USING iceberg AS SELECT * FROM v_order_item;
CREATE TABLE nessie.bronze.cancel         USING iceberg AS SELECT * FROM v_cancel;

-- 검증: 건수 = 랜딩 줄수와 일치해야
SELECT '=== bronze counts ===' AS marker;
SELECT 'payment' AS tbl, COUNT(*) AS cnt FROM nessie.bronze.payment
UNION ALL SELECT 'payment_method', COUNT(*) FROM nessie.bronze.payment_method
UNION ALL SELECT 'order_item', COUNT(*) FROM nessie.bronze.order_item
UNION ALL SELECT 'cancel', COUNT(*) FROM nessie.bronze.cancel
ORDER BY tbl;
