-- 스테이징: 랜딩(payment.p*.ndjson 샤드 전부) → canonical payment (읽기 dedup + event_time 변환)
SELECT txn_id, order_no, brand, amount, type, orig_txn_id, epoch_ms(event_time) AS event_time
FROM (
  SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) AS rn
  FROM read_json_auto('/work/data/landing/payment.p*.ndjson')
)
WHERE rn = 1 AND _op <> 'd'
