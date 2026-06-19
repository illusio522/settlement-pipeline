-- 스테이징: 랜딩(order_item.ndjson) → canonical order_item
SELECT order_no, line_no, partner_id, brand, item_amount
FROM (
  SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) AS rn
  FROM read_json_auto('/work/data/landing/order_item.ndjson')
)
WHERE rn = 1 AND _op <> 'd'
