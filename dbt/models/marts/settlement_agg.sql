-- 집계: (정산일 × 파트너 × 브랜드 × 결제수단) 결제수단별 명세
SELECT settlement_date, partner_id, brand, method,
       COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0) AS gross,
       COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0) AS cancel,
       COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)
         - COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0) AS net,
       COALESCE(SUM(fee) FILTER (WHERE type=1),0) AS fee,
       (COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)
         - COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0))
         - COALESCE(SUM(fee) FILTER (WHERE type=1),0) AS payout,
       COUNT(*) AS txn_count
FROM {{ ref('settlement_event') }}
GROUP BY settlement_date, partner_id, brand, method
