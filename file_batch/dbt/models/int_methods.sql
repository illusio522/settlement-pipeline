SELECT *, SUM(m_amount) OVER (PARTITION BY target_txn_id) AS total FROM {{ ref('int_method_source') }}
