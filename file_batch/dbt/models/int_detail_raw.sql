SELECT a.*, m.method, m.payments_mid,
      ((2 * a.line_alloc::HUGEINT * m.m_amount + m.total) // (2*m.total))::BIGINT AS raw_m
    FROM {{ ref('int_alloc') }} a JOIN {{ ref('int_methods') }} m ON m.target_txn_id=a.txn_id
