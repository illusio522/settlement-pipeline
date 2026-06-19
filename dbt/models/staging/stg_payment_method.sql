-- 스테이징: 랜딩(payment_method.ndjson) → canonical payment_method
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, epoch_ms(event_time) AS event_time
FROM (
  SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) AS rn
  FROM read_json_auto('/work/data/landing/payment_method.ndjson')
)
WHERE rn = 1 AND _op <> 'd'
