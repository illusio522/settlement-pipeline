-- 스테이징: 랜딩(payment_method.p*.ndjson 샤드 전부) → canonical payment_method
-- ★ App-3: incremental — _seq watermark + delete+insert + post_hook 삭제처리 ([[stg_payment]] 동일 패턴)
{{ config(
    materialized='incremental',
    unique_key='pm_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} WHERE _op = 'd'"
) }}

WITH src AS (
  SELECT * FROM read_json_auto('/work/data/landing/payment_method.p*.ndjson')
  {% if is_incremental() %}
  WHERE _seq > (SELECT COALESCE(MAX(_seq), -1) FROM {{ this }})
  {% endif %}
),
deduped AS (
  SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) AS rn FROM src
)
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type,
       epoch_ms(event_time) AS event_time, _op, _seq
FROM deduped
WHERE rn = 1
