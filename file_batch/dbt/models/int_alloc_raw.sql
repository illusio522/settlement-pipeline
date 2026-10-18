SELECT *, CAST(event_time AS DATE) AS settlement_date,
      -- 正수 원 단위 HALF_UP: 부동소수점 나눗셈 없이 정확한 정수 몫 사용.
      ((2 * amount::HUGEINT * item_amount + SUM(item_amount) OVER (PARTITION BY txn_id))
       // (2 * SUM(item_amount) OVER (PARTITION BY txn_id)))::BIGINT AS raw_alloc
    FROM {{ ref('int_pay_line') }}
