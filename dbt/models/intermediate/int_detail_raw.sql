-- 교차 배분 1단계: 라인 × 결제수단
SELECT a.txn_id, a.order_no, a.line_no, a.partner_id, a.brand, a.type, a.event_time, a.settlement_date,
       a.allocated_amount AS line_alloc, om.method, om.payments_mid,
       CAST(ROUND(a.allocated_amount * om.m_amount::DOUBLE / om.order_total) AS BIGINT) AS raw_m
FROM {{ ref('int_alloc') }} a
JOIN {{ ref('int_order_method') }} om ON om.order_no = a.order_no
