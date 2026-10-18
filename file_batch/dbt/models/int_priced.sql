SELECT d.*, f.fee_rate,
       ((2 * d.method_alloc::HUGEINT * CAST(f.fee_rate*10000 AS BIGINT)+10000)//20000)::BIGINT AS fee
    FROM {{ ref('int_detail') }} d ASOF JOIN raw.method_fee f
      ON f.method=d.method AND d.settlement_date>=f.effective_from
