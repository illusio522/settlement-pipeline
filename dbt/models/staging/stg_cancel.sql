-- 스테이징: 랜딩(cancel.p*.ndjson 샤드 전부) → canonical cancel
-- ★ App-3: incremental — 복합키(cancel_txn_id,order_no,line_no), _seq watermark + delete+insert + post_hook
{{ config(
    materialized='incremental',
    unique_key=['cancel_txn_id', 'order_no', 'line_no'],
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} WHERE _op = 'd'"
) }}

WITH src AS (
  SELECT * FROM read_json_auto('/work/data/landing/cancel.p*.ndjson')
  {% if is_incremental() %}
  WHERE _seq > (SELECT COALESCE(MAX(_seq), -1) FROM {{ this }})
  {% endif %}
),
deduped AS (
  SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) AS rn FROM src
)
SELECT cancel_txn_id, order_no, line_no, _op, _seq
FROM deduped
WHERE rn = 1
