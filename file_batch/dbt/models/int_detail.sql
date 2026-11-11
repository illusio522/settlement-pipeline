SELECT *, CASE WHEN method=MAX(method) OVER (PARTITION BY txn_id,line_no)
      THEN line_alloc-(SUM(raw_m) OVER (PARTITION BY txn_id,line_no)-raw_m)
      ELSE raw_m END::BIGINT AS method_alloc
    FROM {{ ref('int_detail_raw') }}
