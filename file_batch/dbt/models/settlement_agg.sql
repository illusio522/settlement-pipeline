WITH sums AS (
  SELECT settlement_date,partner_id,brand,method,
    COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)::BIGINT AS gross,
    COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0)::BIGINT AS cancel,
    COALESCE(SUM(fee) FILTER (WHERE type=1),0)::BIGINT AS fee,
    COUNT(*)::BIGINT AS txn_count
  FROM {{ ref('settlement_event') }}
  GROUP BY settlement_date,partner_id,brand,method
)
SELECT settlement_date,partner_id,brand,method,gross,cancel,
  (gross-cancel)::BIGINT AS net,fee,(gross-cancel-fee)::BIGINT AS payout,txn_count
FROM sums
