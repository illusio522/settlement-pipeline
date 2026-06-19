-- 마스터: payments_mid ↔ brand (주결제수단 MID)
SELECT * FROM (VALUES
  (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2)
) AS t(payments_mid, brand)
