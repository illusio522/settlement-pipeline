-- ============================================================
-- 2단계 소스 DB 초기화 (v3 모델 · 현실적 OLTP 구조)
--   도메인 경계 재현: 결제(paydb) / 주문(orderdb) DB 분리
--   → Debezium 커넥터도 도메인당 1개 (독립 CDC 스트림)
--
--   ★ 결제 도메인 = 헤더-디테일 원장 구조 (실무형)
--     payment(마스터 원장: 모든 수단의 승인/취소 사건, 총액)
--       └ 수단별 원장 4개: payment_card / payment_money / payment_point / payment_coupon
--         (txn_id로 연결 — ★논리 FK. 수단별 고유 속성은 각자 보유. Σ수단 amount = 헤더 amount)
--     수단 확대 = 신규 테이블 추가(예: payment_bank) + 싱크 UNION 추가
--
--   ★ 원칙 (docs/SCHEMA.md):
--     - 결제 원장엔 partner 없음 (partner는 주문 도메인 order_item에서)
--     - ★ 물리 FK 전면 미사용 (논리 FK only) — 고트래픽 결제 원장 관행:
--       FK 참조체크 락경합 회피 · 샤딩/파티셔닝 호환 · JPA(배치 insert·테스트) 편의
--       무결성 보장: 쓰기=앱 트랜잭션(헤더+디테일 같은 Tx) / 사후=정산 대사(고아·Σ수단=헤더 검증)
--     - 도메인 간(paydb↔orderdb)도 당연히 FK 없음 — 별개 시스템. 정합성은 정산 대사가 검증
--     - 마스터(partner·mid_map·method_type·method_fee)는 CDC 아님 → Postgres에 없음
--     - 소스는 현실적으로 풍부하게, 정산은 싱크에서 필요 컬럼만 projection
--       (수단별 4원장 → 싱크에서 UNION+정규화 → canonical payment_method)
--   코드값: type 1=PAY 2=CANCEL · brand 1=mall-a 2=mall-b · 금액 BIGINT(원)
-- ============================================================

-- ---------- 결제 도메인 ----------
CREATE DATABASE paydb;
\connect paydb

-- 결제 마스터 원장 (주문 단위 결제 사건: 승인/취소, 총액. partner·MID 없음)
CREATE TABLE payment (
    txn_id       BIGINT PRIMARY KEY,                      -- 거래 ID
    order_no     BIGINT       NOT NULL,
    brand        SMALLINT     NOT NULL,                   -- 1=mall-a 2=mall-b
    amount       BIGINT       NOT NULL,                   -- 결제 총액(원)
    currency     CHAR(3)      NOT NULL DEFAULT 'KRW',
    type         SMALLINT     NOT NULL,                   -- 1=PAY 2=CANCEL
    orig_txn_id  BIGINT      , -- 취소의 원거래
    status       SMALLINT     NOT NULL DEFAULT 1,         -- 1=APPROVED 2=CANCELED 9=FAILED
    event_time   TIMESTAMP    NOT NULL,                   -- 거래시각(정산 기준)
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_payment_event_time ON payment(event_time);
CREATE INDEX idx_payment_order ON payment(order_no);

-- 카드 원장 (주결제수단 · PG 정산 대상 → MID)
CREATE TABLE payment_card (
    pm_id              BIGINT PRIMARY KEY,
    txn_id             BIGINT      NOT NULL,
    order_no           BIGINT      NOT NULL,
    payments_mid       INTEGER     NOT NULL,              -- 페이먼츠 MID
    amount             BIGINT      NOT NULL,
    type               SMALLINT    NOT NULL,              -- 1=PAY 2=CANCEL
    card_company       VARCHAR(20),                       -- 카드사
    pg_txn_id          VARCHAR(40),                       -- PG사 거래번호
    approval_no        VARCHAR(20),                       -- 승인번호
    installment_months SMALLINT    NOT NULL DEFAULT 0,    -- 할부(0=일시불)
    event_time         TIMESTAMP   NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_card_txn ON payment_card(txn_id);

-- 머니 원장 (주결제수단 · PG 정산 대상 → MID)
CREATE TABLE payment_money (
    pm_id          BIGINT PRIMARY KEY,
    txn_id         BIGINT      NOT NULL,
    order_no       BIGINT      NOT NULL,
    payments_mid   INTEGER     NOT NULL,
    amount         BIGINT      NOT NULL,
    type           SMALLINT    NOT NULL,
    wallet_id      BIGINT,                                -- 머니 지갑(계정)
    balance_txn_id VARCHAR(40),                           -- 잔액 거래번호
    event_time     TIMESTAMP   NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_money_txn ON payment_money(txn_id);

-- 포인트 원장 (보조 · 플랫폼 부담 → MID 없음)
CREATE TABLE payment_point (
    pm_id           BIGINT PRIMARY KEY,
    txn_id          BIGINT      NOT NULL,
    order_no        BIGINT      NOT NULL,
    amount          BIGINT      NOT NULL,
    type            SMALLINT    NOT NULL,
    point_wallet_id BIGINT,                               -- 포인트 지갑
    event_time      TIMESTAMP   NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_point_txn ON payment_point(txn_id);

-- 쿠폰 원장 (보조 · 플랫폼 부담 → MID 없음)
CREATE TABLE payment_coupon (
    pm_id      BIGINT PRIMARY KEY,
    txn_id     BIGINT      NOT NULL,
    order_no   BIGINT      NOT NULL,
    amount     BIGINT      NOT NULL,
    type       SMALLINT    NOT NULL,
    coupon_id  BIGINT,                                    -- 사용 쿠폰
    policy_id  BIGINT,                                    -- 쿠폰 정책
    event_time TIMESTAMP   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_coupon_txn ON payment_coupon(txn_id);

-- Debezium publication (pgoutput) — 결제 도메인 스트림 (5개 테이블)
CREATE PUBLICATION dbz_pay FOR TABLE payment, payment_card, payment_money, payment_point, payment_coupon;

-- ---------- 주문 도메인 ----------
\connect postgres
CREATE DATABASE orderdb;
\connect orderdb

-- 주문상품 라인 (★ partner 귀속은 여기서)
CREATE TABLE order_item (
    order_no        BIGINT      NOT NULL,
    line_no         SMALLINT    NOT NULL,
    product_id      BIGINT      NOT NULL,
    qty             INTEGER     NOT NULL DEFAULT 1,
    unit_price      BIGINT      NOT NULL,
    item_amount     BIGINT      NOT NULL,                 -- qty×unit_price (정산 라인 금액)
    discount_amount BIGINT      NOT NULL DEFAULT 0,       -- 할인(향후 안분 배분 확장용)
    partner_id      INTEGER     NOT NULL,                 -- 입점업체(정산 대상)
    brand           SMALLINT    NOT NULL,
    status          SMALLINT    NOT NULL DEFAULT 1,       -- 1=ORDERED 2=CANCELED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_no, line_no)
);
CREATE INDEX idx_oi_partner ON order_item(partner_id);

-- 취소 라인 명세 (부분취소: 어느 주문의 어느 라인이 취소됐나)
CREATE TABLE cancel (
    cancel_txn_id BIGINT      NOT NULL,
    order_no      BIGINT      NOT NULL,
    line_no       SMALLINT    NOT NULL,
    reason_code   SMALLINT    NOT NULL DEFAULT 1,         -- 1=고객변심 2=품절 3=불량
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cancel_txn_id, order_no, line_no)
);

-- Debezium publication — 주문 도메인 스트림
CREATE PUBLICATION dbz_order FOR TABLE order_item, cancel;
