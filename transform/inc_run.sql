-- 3-4: 증분 정산 실행 (1회 = 1배치). data/incremental.duckdb 에 반복 적용.
--   ① 랜딩에서 _seq > watermark 인 새 레코드만 (dedup, event_time 변환)
--   ② 새 canonical → durable 누적
--   ③ 영향 주문 식별 → ④ 그 주문만 재정산(event 누적) → ⑤ agg 재계산 → ⑥ watermark 갱신
--   영향 주문 단위로 durable canonical 전체에서 재계산 → 취소(구 주문)도 정확.

-- ① 새 레코드 (스트림별 _seq > watermark)
CREATE OR REPLACE TEMP TABLE _new_payment AS
SELECT txn_id, order_no, brand, amount, type, orig_txn_id, epoch_ms(event_time) AS event_time, _seq
FROM (SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/payment.p*.ndjson')
      WHERE _seq > (SELECT last_seq FROM _watermark WHERE stream='payment'))
WHERE rn=1 AND _op<>'d';

CREATE OR REPLACE TEMP TABLE _new_method AS
SELECT pm_id, txn_id, order_no, method, payments_mid, amount, type, epoch_ms(event_time) AS event_time, _seq
FROM (SELECT *, row_number() OVER (PARTITION BY pm_id ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/payment_method.p*.ndjson')
      WHERE _seq > (SELECT last_seq FROM _watermark WHERE stream='payment_method'))
WHERE rn=1 AND _op<>'d';

CREATE OR REPLACE TEMP TABLE _new_item AS
SELECT order_no, line_no, partner_id, brand, item_amount, _seq
FROM (SELECT *, row_number() OVER (PARTITION BY order_no, line_no ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/order_item.p*.ndjson')
      WHERE _seq > (SELECT last_seq FROM _watermark WHERE stream='order_item'))
WHERE rn=1 AND _op<>'d';

CREATE OR REPLACE TEMP TABLE _new_cancel AS
SELECT cancel_txn_id, order_no, line_no, _seq
FROM (SELECT *, row_number() OVER (PARTITION BY cancel_txn_id, order_no, line_no ORDER BY _seq DESC) rn
      FROM read_json_auto('/work/data/landing/cancel.p*.ndjson')
      WHERE _seq > (SELECT last_seq FROM _watermark WHERE stream='cancel'))
WHERE rn=1 AND _op<>'d';

.print '── 이번 배치 새 레코드 수 (watermark 이후만) ──'
SELECT (SELECT COUNT(*) FROM _new_payment) AS new_payment,
       (SELECT COUNT(*) FROM _new_method)  AS new_method,
       (SELECT COUNT(*) FROM _new_item)    AS new_item,
       (SELECT COUNT(*) FROM _new_cancel)  AS new_cancel;

-- ② 새 canonical → durable 누적
INSERT INTO payment        SELECT txn_id,order_no,brand,amount,type,orig_txn_id,event_time FROM _new_payment;
INSERT INTO payment_method SELECT pm_id,txn_id,order_no,method,payments_mid,amount,type,event_time FROM _new_method;
INSERT INTO order_item     SELECT order_no,line_no,partner_id,brand,item_amount FROM _new_item;
INSERT INTO cancel         SELECT cancel_txn_id,order_no,line_no FROM _new_cancel;

-- ③ 영향받은 주문
CREATE OR REPLACE TEMP TABLE _affected AS
SELECT DISTINCT order_no FROM _new_payment
UNION SELECT DISTINCT order_no FROM _new_cancel;

-- ④ 영향 주문의 기존 event 제거 후 재계산 (멱등) — durable canonical 전체 기준
DELETE FROM settlement_event WHERE order_no IN (SELECT order_no FROM _affected);

CREATE OR REPLACE TEMP TABLE _pay_line AS
SELECT p.txn_id, p.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount AS pay_amount
FROM payment p JOIN order_item oi ON oi.order_no=p.order_no
WHERE p.type=1 AND p.order_no IN (SELECT order_no FROM _affected)
UNION ALL
SELECT p.txn_id, c.order_no, oi.line_no, oi.partner_id, oi.brand, oi.item_amount, p.type, p.event_time, p.amount
FROM payment p JOIN cancel c ON c.cancel_txn_id=p.txn_id
JOIN order_item oi ON oi.order_no=c.order_no AND oi.line_no=c.line_no
WHERE p.type=2 AND c.order_no IN (SELECT order_no FROM _affected);

CREATE OR REPLACE TEMP TABLE _alloc_raw AS
SELECT *, CAST(event_time AS DATE) AS settlement_date,
       CAST(ROUND(pay_amount * item_amount::DOUBLE / SUM(item_amount) OVER (PARTITION BY txn_id)) AS BIGINT) AS raw_alloc
FROM _pay_line;
CREATE OR REPLACE TEMP TABLE _alloc AS
SELECT *, CASE WHEN line_no = MAX(line_no) OVER (PARTITION BY txn_id)
       THEN pay_amount - (SUM(raw_alloc) OVER (PARTITION BY txn_id) - raw_alloc) ELSE raw_alloc END AS allocated_amount
FROM _alloc_raw;

CREATE OR REPLACE TEMP TABLE _order_method AS
SELECT order_no, method, payments_mid, amount AS m_amount,
       SUM(amount) OVER (PARTITION BY order_no) AS order_total
FROM payment_method WHERE type=1 AND order_no IN (SELECT order_no FROM _affected);

CREATE OR REPLACE TEMP TABLE _detail_raw AS
SELECT a.txn_id, a.order_no, a.line_no, a.partner_id, a.brand, a.type, a.event_time, a.settlement_date,
       a.allocated_amount AS line_alloc, om.method, om.payments_mid,
       CAST(ROUND(a.allocated_amount * om.m_amount::DOUBLE / om.order_total) AS BIGINT) AS raw_m
FROM _alloc a JOIN _order_method om ON om.order_no = a.order_no;
CREATE OR REPLACE TEMP TABLE _detail AS
SELECT *, CASE WHEN method = MAX(method) OVER (PARTITION BY txn_id, line_no)
       THEN line_alloc - (SUM(raw_m) OVER (PARTITION BY txn_id, line_no) - raw_m) ELSE raw_m END AS method_alloc
FROM _detail_raw;

INSERT INTO settlement_event
SELECT d.brand||':'||d.txn_id||':'||d.line_no||':'||d.method, d.txn_id, d.order_no, d.line_no,
       d.partner_id, d.brand, d.type, d.method, d.payments_mid,
       d.method_alloc, f.fee_rate, CAST(ROUND(d.method_alloc*f.fee_rate) AS BIGINT) AS fee,
       d.method_alloc - CAST(ROUND(d.method_alloc*f.fee_rate) AS BIGINT) AS net_amount, d.event_time, d.settlement_date
FROM _detail d
ASOF JOIN method_fee f ON f.method = d.method AND d.settlement_date >= f.effective_from;

-- ⑤ settlement_agg 재계산 (전체 — agg는 작음)
TRUNCATE settlement_agg;
INSERT INTO settlement_agg
SELECT settlement_date, partner_id, brand, method,
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0),
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0),
  COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0),
  COALESCE(SUM(fee) FILTER (WHERE type=1),0),
  (COALESCE(SUM(allocated_amount) FILTER (WHERE type=1),0)-COALESCE(SUM(allocated_amount) FILTER (WHERE type=2),0))-COALESCE(SUM(fee) FILTER (WHERE type=1),0),
  COUNT(*)
FROM settlement_event GROUP BY settlement_date, partner_id, brand, method;

-- ⑥ watermark 갱신 (스트림별 최대 _seq)
UPDATE _watermark SET last_seq = GREATEST(last_seq, COALESCE((SELECT MAX(_seq) FROM _new_payment), last_seq)) WHERE stream='payment';
UPDATE _watermark SET last_seq = GREATEST(last_seq, COALESCE((SELECT MAX(_seq) FROM _new_method),  last_seq)) WHERE stream='payment_method';
UPDATE _watermark SET last_seq = GREATEST(last_seq, COALESCE((SELECT MAX(_seq) FROM _new_item),    last_seq)) WHERE stream='order_item';
UPDATE _watermark SET last_seq = GREATEST(last_seq, COALESCE((SELECT MAX(_seq) FROM _new_cancel),  last_seq)) WHERE stream='cancel';

.print '── watermark (갱신 후) ──'
SELECT stream, last_seq FROM _watermark ORDER BY stream;
.print '── 현재 누적 결과 ──'
SELECT (SELECT COUNT(*) FROM settlement_event) AS events, (SELECT COUNT(*) FROM settlement_agg) AS agg;
