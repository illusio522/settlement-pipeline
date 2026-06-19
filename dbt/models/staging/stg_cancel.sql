-- 스테이징: 랜딩(cancel.p*.ndjson 샤드 전부) → canonical cancel
SELECT cancel_txn_id, order_no, line_no
FROM (
  SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) AS rn
  FROM read_json_auto('/work/data/landing/cancel.p*.ndjson')
)
WHERE rn = 1 AND _op <> 'd'
