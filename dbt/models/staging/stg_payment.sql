-- 스테이징: 랜딩(payment.p*.ndjson 샤드 전부) → canonical payment (읽기 dedup + event_time 변환)
-- ★ App-3: incremental — _seq(LSN) watermark로 새 랜딩 행만 처리, delete+insert로 upsert.
--   _op·_seq 컬럼 유지(watermark·삭제처리용). post_hook이 삭제마커(_op='d')를 제거
--   → dbt 네이티브가 하드삭제를 처리 못 하는 한계 보완. 첫 실행=전체, 이후=증분.
{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='delete+insert',
    post_hook="DELETE FROM {{ this }} WHERE _op = 'd'"
) }}

WITH src AS (
  SELECT * FROM read_json_auto('/work/data/landing/payment.p*.ndjson')
  {% if is_incremental() %}
  WHERE _seq > (SELECT COALESCE(MAX(_seq), -1) FROM {{ this }})   -- {{this}}=기존 테이블=watermark
  {% endif %}
),
deduped AS (
  SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) AS rn FROM src
)
SELECT txn_id, order_no, brand, amount, type, orig_txn_id,
       epoch_ms(event_time) AS event_time, _op, _seq
FROM deduped
WHERE rn = 1   -- 삭제마커('d')는 남겨서 delete+insert가 키를 지우게 → post_hook이 마커 제거
