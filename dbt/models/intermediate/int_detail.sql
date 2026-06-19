-- 교차 배분 2단계: 라인 내 결제수단 잔액보정 (돈 보존)
SELECT *,
       CASE WHEN method = MAX(method) OVER (PARTITION BY txn_id, line_no)
            THEN line_alloc - (SUM(raw_m) OVER (PARTITION BY txn_id, line_no) - raw_m)
            ELSE raw_m END AS method_alloc
FROM {{ ref('int_detail_raw') }}
