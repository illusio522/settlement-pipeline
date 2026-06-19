-- FREEZE: 결제수단별 as-of 수수료 박제 → 불변 원장 (라인 × 결제수단)
SELECT d.brand || ':' || d.txn_id || ':' || d.line_no || ':' || d.method AS event_id,
       d.txn_id, d.order_no, d.line_no, d.partner_id, d.brand, d.type, d.method, d.payments_mid,
       d.method_alloc AS allocated_amount,
       f.fee_rate,
       CAST(ROUND(d.method_alloc * f.fee_rate) AS BIGINT) AS fee,
       d.method_alloc - CAST(ROUND(d.method_alloc * f.fee_rate) AS BIGINT) AS net_amount,
       d.event_time, d.settlement_date
FROM {{ ref('int_detail') }} d
ASOF JOIN {{ ref('method_fee') }} f
  ON f.method = d.method AND d.settlement_date >= f.effective_from
