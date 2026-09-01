-- 서빙 스토어 DDL (servedb) — 일 마감 배치가 TRUNCATE+COPY로 재적재하는 조회 전용 테이블.
--   publish-serving.sh 가 매 실행 시 적용 (IF NOT EXISTS 멱등).
CREATE TABLE IF NOT EXISTS settlement_agg (
    settlement_date DATE     NOT NULL,
    partner_id      INT      NOT NULL,
    brand           SMALLINT NOT NULL,
    method          SMALLINT NOT NULL,   -- 1=CARD 2=MONEY 3=POINT 4=COUPON
    gross           BIGINT   NOT NULL,
    cancel          BIGINT   NOT NULL,
    net             BIGINT   NOT NULL,
    fee             BIGINT   NOT NULL,
    payout          BIGINT   NOT NULL,
    txn_count       BIGINT   NOT NULL,
    PRIMARY KEY (settlement_date, partner_id, brand, method)
);
