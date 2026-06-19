-- 마스터: 결제수단별 차등 수수료 (버전드, as-of)
SELECT * FROM (VALUES
  (1, 0.0250, DATE '2020-01-01'),
  (2, 0.0150, DATE '2020-01-01'),
  (3, 0.0000, DATE '2020-01-01'),
  (4, 0.0000, DATE '2020-01-01'),
  (1, 0.0230, DATE '2026-06-01')
) AS t(method, fee_rate, effective_from)
