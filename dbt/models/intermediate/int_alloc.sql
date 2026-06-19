-- 라인 배분 2단계: 마지막 라인 잔액보정 (돈 보존)
SELECT *,
       CASE WHEN line_no = MAX(line_no) OVER (PARTITION BY txn_id)
            THEN pay_amount - (SUM(raw_alloc) OVER (PARTITION BY txn_id) - raw_alloc)
            ELSE raw_alloc END AS allocated_amount
FROM {{ ref('int_alloc_raw') }}
