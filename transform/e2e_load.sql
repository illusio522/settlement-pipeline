-- 2-5 작업2: 랜딩(NDJSON) → canonical 4테이블 재구성 (읽기 시점 dedup)
--   ① 마스터(partner·method_fee·mid_map) = 스냅샷(CDC 아님) → 1단계와 동일하게 적재
--   ② 소스 = 랜딩 읽어 key별 _seq 최대 & _op<>'d' (중복 제거 + 삭제 반영)
--   ③ event_time = epoch밀리초(Long) → epoch_ms()로 TIMESTAMP 복원
-- 전제: transform/01_schema.sql 로 테이블·method_type 생성 후 실행.

-- ===== ① 마스터 (1단계 02_seed와 동일) =====
INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);

INSERT INTO method_fee VALUES
  (1, 0.0250, DATE '2020-01-01'), (2, 0.0150, DATE '2020-01-01'),
  (3, 0.0000, DATE '2020-01-01'), (4, 0.0000, DATE '2020-01-01'),
  (1, 0.0230, DATE '2026-06-01');

INSERT INTO mid_map VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2);

-- ===== ② 랜딩 → canonical (dedup: key별 최신 _seq, 삭제 제외) =====
INSERT INTO payment
SELECT txn_id, order_no, brand, amount, type, orig_txn_id, epoch_ms(event_time)
FROM (SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/payment.ndjson'))
WHERE rn=1 AND _op<>'d';

INSERT INTO payment_method
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, epoch_ms(event_time)
FROM (SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/payment_method.ndjson'))
WHERE rn=1 AND _op<>'d';

INSERT INTO order_item
SELECT order_no, line_no, partner_id, brand, item_amount
FROM (SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/order_item.ndjson'))
WHERE rn=1 AND _op<>'d';

INSERT INTO cancel
SELECT cancel_txn_id, order_no, line_no
FROM (SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/cancel.ndjson'))
WHERE rn=1 AND _op<>'d';

-- ===== ③ 고아 검증 (물리 FK 없앤 대신 — 파이프라인이 고아 안 만들었나) =====
.print ''
.print '── 고아 검증 (전부 0이어야) ──'
SELECT
  (SELECT COUNT(*) FROM payment_method pm LEFT JOIN payment p ON p.txn_id=pm.txn_id WHERE p.txn_id IS NULL) AS pm_no_header,
  (SELECT COUNT(*) FROM cancel c LEFT JOIN order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no WHERE oi.order_no IS NULL) AS cancel_no_line,
  (SELECT COUNT(*) FROM (
      SELECT p.txn_id FROM payment p JOIN payment_method pm ON pm.txn_id=p.txn_id AND pm.type=p.type
      GROUP BY p.txn_id, p.amount HAVING p.amount <> SUM(pm.amount)
   )) AS sum_method_ne_header;
.print ''
