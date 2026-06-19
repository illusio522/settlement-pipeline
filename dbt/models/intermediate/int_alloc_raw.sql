-- 라인 배분 1단계: 상품가 비율 (윈도우 분리 — int_alloc과 별 모델로 크래시 회피)
SELECT *,
       CAST(event_time AS DATE) AS settlement_date,
       CAST(ROUND(pay_amount * item_amount::DOUBLE / SUM(item_amount) OVER (PARTITION BY txn_id)) AS BIGINT) AS raw_alloc
FROM {{ ref('int_pay_line') }}
