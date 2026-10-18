SELECT p.*, oi.line_no, oi.partner_id, oi.item_amount
    FROM raw.payment p JOIN raw.order_item oi USING(order_no)
    WHERE p.type=1
    UNION ALL
    SELECT p.*, oi.line_no, oi.partner_id, oi.item_amount
    FROM raw.payment p JOIN raw.cancel c ON c.cancel_txn_id=p.txn_id AND c.order_no=p.order_no
    JOIN raw.order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
    WHERE p.type=2
