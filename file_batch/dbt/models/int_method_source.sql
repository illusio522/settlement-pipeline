SELECT p.txn_id AS target_txn_id, m.method, m.payments_mid, SUM(m.amount)::BIGINT AS m_amount
    FROM raw.payment p JOIN raw.payment_method m ON m.txn_id=p.txn_id
    WHERE p.type=1 GROUP BY p.txn_id,m.method,m.payments_mid
    UNION ALL
    SELECT p.txn_id, m.method, m.payments_mid, SUM(m.amount)::BIGINT
    FROM raw.payment p JOIN raw.original_payment_method m ON m.txn_id=p.orig_txn_id
    WHERE p.type=2 GROUP BY p.txn_id,m.method,m.payments_mid
