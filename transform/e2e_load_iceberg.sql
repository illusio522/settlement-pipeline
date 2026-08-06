-- 3-2: Iceberg 테이블(MinIO) → canonical 4테이블 적재.
--   dedup·event_time 변환은 build-iceberg.sh(쓰기 시점)에 이미 적용됨 → 여기선 직접 읽기.
-- 전제: 01_schema.sql 로 테이블 생성 후 같은 세션 실행.

-- httpfs·S3 접속은 러너(scripts/run-e2e-iceberg.sh)가 infra/.env 값으로 앞에 붙여 넣는다.
INSTALL iceberg; LOAD iceberg;
SET unsafe_enable_version_guessing=true;

-- 마스터 (스냅샷)
INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);
INSERT INTO method_fee VALUES
  (1,0.0250,DATE '2020-01-01'),(2,0.0150,DATE '2020-01-01'),
  (3,0.0000,DATE '2020-01-01'),(4,0.0000,DATE '2020-01-01'),(1,0.0230,DATE '2026-06-01');
INSERT INTO mid_map VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2);

-- Iceberg 테이블 → canonical (이미 dedup된 상태)
INSERT INTO payment        SELECT * FROM iceberg_scan('s3://lake/iceberg/payment');
INSERT INTO payment_method SELECT * FROM iceberg_scan('s3://lake/iceberg/payment_method');
INSERT INTO order_item     SELECT * FROM iceberg_scan('s3://lake/iceberg/order_item');
INSERT INTO cancel         SELECT * FROM iceberg_scan('s3://lake/iceberg/cancel');
