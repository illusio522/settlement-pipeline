package com.settlement.serving.domain;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * 일 정산 집계 (조회 전용 — 쓰기는 배치의 TRUNCATE+COPY 뿐).
 *  ID 참조 패턴: partner 등 연관 엔티티 없음 (프로젝트 확정 설계).
 */
@Entity
@Table(name = "settlement_agg")
public class SettlementAgg {

    @EmbeddedId
    private SettlementAggId id;

    private Long gross;
    private Long cancel;
    private Long net;
    private Long fee;
    private Long payout;
    private Long txnCount;

    protected SettlementAgg() { }   // JPA 기본 생성자

    public SettlementAggId getId() { return id; }
    public Long getGross() { return gross; }
    public Long getCancel() { return cancel; }
    public Long getNet() { return net; }
    public Long getFee() { return fee; }
    public Long getPayout() { return payout; }
    public Long getTxnCount() { return txnCount; }
}
