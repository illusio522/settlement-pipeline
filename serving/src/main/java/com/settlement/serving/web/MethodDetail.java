package com.settlement.serving.web;

/** 특정 파트너 일 정산의 결제수단별 상세. */
public record MethodDetail(
        Short brand,
        Short method,
        String methodName,
        Long gross,
        Long cancel,
        Long net,
        Long fee,
        Long payout,
        Long txnCount
) {
    /** method 코드 → 이름 (method_type 마스터의 코드 체계). */
    public static String nameOf(Short method) {
        return switch (method) {
            case 1 -> "CARD";
            case 2 -> "MONEY";
            case 3 -> "POINT";
            case 4 -> "COUPON";
            default -> "UNKNOWN";
        };
    }
}
