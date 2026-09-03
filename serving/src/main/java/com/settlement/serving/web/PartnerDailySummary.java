package com.settlement.serving.web;

/** 일자별 파트너 요약 (결제수단 합산). JPQL 생성자 프로젝션. */
public record PartnerDailySummary(
        Integer partnerId,
        Long gross,
        Long cancel,
        Long net,
        Long fee,
        Long payout,
        Long txnCount
) { }
