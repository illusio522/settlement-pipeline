-- 청구 라인: PAY=전 라인, CANCEL=취소 라인만
SELECT p.txn_id, p.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount AS pay_amount
FROM {{ ref('stg_payment') }} p
JOIN {{ ref('stg_order_item') }} oi ON oi.order_no = p.order_no
WHERE p.type = 1
UNION ALL
SELECT p.txn_id, c.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount
FROM {{ ref('stg_payment') }} p
JOIN {{ ref('stg_cancel') }} c ON c.cancel_txn_id = p.txn_id
JOIN {{ ref('stg_order_item') }} oi ON oi.order_no = c.order_no AND oi.line_no = c.line_no
WHERE p.type = 2
