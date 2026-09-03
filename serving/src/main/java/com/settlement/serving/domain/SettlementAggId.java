package com.settlement.serving.domain;

import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Objects;

/** settlement_agg 복합키 (정산일 + 파트너 + 브랜드 + 결제수단). */
@Embeddable
public class SettlementAggId implements Serializable {

    private LocalDate settlementDate;
    private Integer partnerId;
    private Short brand;
    private Short method;

    protected SettlementAggId() { }   // JPA 기본 생성자

    public SettlementAggId(LocalDate settlementDate, Integer partnerId, Short brand, Short method) {
        this.settlementDate = settlementDate;
        this.partnerId = partnerId;
        this.brand = brand;
        this.method = method;
    }

    public LocalDate getSettlementDate() { return settlementDate; }
    public Integer getPartnerId() { return partnerId; }
    public Short getBrand() { return brand; }
    public Short getMethod() { return method; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SettlementAggId that)) return false;
        return Objects.equals(settlementDate, that.settlementDate)
                && Objects.equals(partnerId, that.partnerId)
                && Objects.equals(brand, that.brand)
                && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settlementDate, partnerId, brand, method);
    }
}
