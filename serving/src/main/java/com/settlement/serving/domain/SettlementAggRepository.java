package com.settlement.serving.domain;

import com.settlement.serving.web.PartnerDailySummary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;

public interface SettlementAggRepository extends JpaRepository<SettlementAgg, SettlementAggId> {

    /** 일자별 파트너 요약 (수단 합산, 파트너 오름차순 페이징). */
    @Query(value = """
            SELECT new com.settlement.serving.web.PartnerDailySummary(
                a.id.partnerId, SUM(a.gross), SUM(a.cancel), SUM(a.net),
                SUM(a.fee), SUM(a.payout), SUM(a.txnCount))
            FROM SettlementAgg a
            WHERE a.id.settlementDate = :date
            GROUP BY a.id.partnerId
            ORDER BY a.id.partnerId
            """,
            countQuery = """
            SELECT COUNT(DISTINCT a.id.partnerId)
            FROM SettlementAgg a
            WHERE a.id.settlementDate = :date
            """)
    Page<PartnerDailySummary> summarizeByDate(@Param("date") LocalDate date, Pageable pageable);

    /** 특정 파트너의 일 정산 상세 (결제수단별 행). */
    List<SettlementAgg> findByIdSettlementDateAndIdPartnerIdOrderByIdBrandAscIdMethodAsc(
            LocalDate settlementDate, Integer partnerId);
}
