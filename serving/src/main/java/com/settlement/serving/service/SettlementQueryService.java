package com.settlement.serving.service;

import com.settlement.serving.domain.SettlementAgg;
import com.settlement.serving.domain.SettlementAggRepository;
import com.settlement.serving.web.MethodDetail;
import com.settlement.serving.web.PartnerDailySummary;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;

/** 정산 조회 (read-only — 쓰기는 배치만 한다). */
@Service
@Transactional(readOnly = true)
public class SettlementQueryService {

    private final SettlementAggRepository repository;

    public SettlementQueryService(SettlementAggRepository repository) {
        this.repository = repository;
    }

    public Page<PartnerDailySummary> dailySummary(LocalDate date, Pageable pageable) {
        return repository.summarizeByDate(date, pageable);
    }

    public List<MethodDetail> partnerDetail(LocalDate date, Integer partnerId) {
        return repository
                .findByIdSettlementDateAndIdPartnerIdOrderByIdBrandAscIdMethodAsc(date, partnerId)
                .stream()
                .map(SettlementQueryService::toDetail)
                .toList();
    }

    private static MethodDetail toDetail(SettlementAgg a) {
        return new MethodDetail(
                a.getId().getBrand(), a.getId().getMethod(), MethodDetail.nameOf(a.getId().getMethod()),
                a.getGross(), a.getCancel(), a.getNet(), a.getFee(), a.getPayout(), a.getTxnCount());
    }
}
