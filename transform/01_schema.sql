-- 1단계 스키마 v3 — 복합결제 + 결제수단별 정산 명세 + 차등 수수료
-- type 1=PAY 2=CANCEL · brand 1=mall-a 2=mall-b · status 0=MATCHED 1=UNMATCHED · 금액 BIGINT(원)

-- 결제수단 마스터 (확대 용이: 행 추가)
CREATE OR REPLACE TABLE method_type (
  method_code SMALLINT PRIMARY KEY, method_name VARCHAR, category VARCHAR, is_pg BOOLEAN
);
INSERT INTO method_type VALUES
  (1,'CARD','MAIN',TRUE),(2,'MONEY','MAIN',TRUE),(3,'POINT','AUX',FALSE),(4,'COUPON','AUX',FALSE);

-- 결제수단별 정산 수수료율(버전드, as-of) — ★ 차등 수수료
CREATE OR REPLACE TABLE method_fee (
  method SMALLINT, fee_rate DECIMAL(5,4), effective_from DATE
);

-- ===== 소스: 결제 도메인 =====
CREATE OR REPLACE TABLE payment (   -- 결제 헤더(통합)
  txn_id BIGINT PRIMARY KEY, order_no BIGINT, brand SMALLINT, amount BIGINT,
  type SMALLINT, orig_txn_id BIGINT, event_time TIMESTAMP
);
CREATE OR REPLACE TABLE payment_method (  -- 결제수단별 승인/취소
  pm_id BIGINT PRIMARY KEY, txn_id BIGINT, order_no BIGINT, method SMALLINT,
  payments_mid INTEGER, amount BIGINT, type SMALLINT, event_time TIMESTAMP
);

-- ===== 소스: 주문 도메인 =====
CREATE OR REPLACE TABLE order_item (
  order_no BIGINT, line_no SMALLINT, partner_id INTEGER, brand SMALLINT, item_amount BIGINT
);
CREATE OR REPLACE TABLE cancel (cancel_txn_id BIGINT, order_no BIGINT, line_no SMALLINT);

-- ===== 마스터 =====
CREATE OR REPLACE TABLE partner (partner_id INTEGER PRIMARY KEY, name VARCHAR, brand SMALLINT);
CREATE OR REPLACE TABLE mid_map (payments_mid INTEGER PRIMARY KEY, brand SMALLINT);

-- ===== 외부: PG 정산파일(대사) =====
CREATE OR REPLACE TABLE pg_settlement_file (settlement_date DATE, payments_mid INTEGER, pg_amount BIGINT);

-- ===== 정산 파생 =====
-- ★ (라인 × 결제수단) 단위 불변 원장 = 결제수단별 백데이터
CREATE OR REPLACE TABLE settlement_event (
  event_id VARCHAR, txn_id BIGINT, order_no BIGINT, line_no SMALLINT,
  partner_id INTEGER, brand SMALLINT, type SMALLINT,
  method SMALLINT, payments_mid INTEGER,
  allocated_amount BIGINT, fee_rate DECIMAL(5,4), fee BIGINT, net_amount BIGINT,
  event_time TIMESTAMP, settlement_date DATE
);
-- 집계: (정산일 × 파트너 × 브랜드 × 결제수단) → 결제수단별 명세
CREATE OR REPLACE TABLE settlement_agg (
  settlement_date DATE, partner_id INTEGER, brand SMALLINT, method SMALLINT,
  gross BIGINT, cancel BIGINT, net BIGINT, fee BIGINT, payout BIGINT, txn_count BIGINT
);
CREATE OR REPLACE TABLE recon_result (   -- 파트너 보존
  settlement_date DATE, brand SMALLINT, src_pay BIGINT, agg_pay BIGINT, diff BIGINT, status SMALLINT
);
CREATE OR REPLACE TABLE recon_card (     -- 주결제수단 MID 대사
  settlement_date DATE, payments_mid INTEGER, card_total BIGINT, pg_total BIGINT, diff BIGINT, status SMALLINT
);
