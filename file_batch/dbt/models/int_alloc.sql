SELECT *, CASE WHEN line_no=MAX(line_no) OVER (PARTITION BY txn_id)
      THEN amount-(SUM(raw_alloc) OVER (PARTITION BY txn_id)-raw_alloc)
      ELSE raw_alloc END::BIGINT AS line_alloc
    FROM {{ ref('int_alloc_raw') }}
