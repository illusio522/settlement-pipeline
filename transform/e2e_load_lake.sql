-- 3-1: 랜딩(MinIO Parquet) → canonical 4테이블 재구성 (읽기 dedup)
--   e2e_load.sql 의 레이크 버전 — 로컬 NDJSON 대신 s3:// Parquet 읽기.
-- 전제: transform/01_schema.sql 로 테이블 생성 후, 같은 세션에서 실행.

-- MinIO(s3) 접속은 러너(scripts/run-e2e-lake.sh)가 infra/.env 값으로 앞에 붙여 넣는다.

-- ① 마스터 (스냅샷, 1단계와 동일)
INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);
INSERT INTO method_fee VALUES
  (1,0.0250,DATE '2020-01-01'),(2,0.0150,DATE '2020-01-01'),
  (3,0.0000,DATE '2020-01-01'),(4,0.0000,DATE '2020-01-01'),(1,0.0230,DATE '2026-06-01');
INSERT INTO mid_map VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2);

-- ② 레이크(Parquet) → canonical (key별 최신 _seq, 삭제 제외)
INSERT INTO payment
SELECT txn_id, order_no, brand, amount, type, orig_txn_id, epoch_ms(event_time)
FROM (SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) rn
      FROM 's3://lake/landing/payment.parquet') WHERE rn=1 AND _op<>'d';

INSERT INTO payment_method
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, epoch_ms(event_time)
FROM (SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) rn
      FROM 's3://lake/landing/payment_method.parquet') WHERE rn=1 AND _op<>'d';

INSERT INTO order_item
SELECT order_no, line_no, partner_id, brand, item_amount
FROM (SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) rn
      FROM 's3://lake/landing/order_item.parquet') WHERE rn=1 AND _op<>'d';

INSERT INTO cancel
SELECT cancel_txn_id, order_no, line_no
FROM (SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) rn
      FROM 's3://lake/landing/cancel.parquet') WHERE rn=1 AND _op<>'d';
