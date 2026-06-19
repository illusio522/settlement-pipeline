-- 주문별 결제수단 비율 (PAY 기준)
SELECT order_no, method, payments_mid, amount AS m_amount,
       SUM(amount) OVER (PARTITION BY order_no) AS order_total
FROM {{ ref('stg_payment_method') }}
WHERE type = 1
