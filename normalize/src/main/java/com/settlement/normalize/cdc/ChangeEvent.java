package com.settlement.normalize.cdc;

import java.util.Map;

/**
 * 파싱 완료된 변경 이벤트 (정규화의 입력).
 *   sourceTable : 소스 테이블명 (payment, payment_card, order_item, cancel ...)
 *   op          : 변경 종류
 *   row         : 유효한 행 (c/u/r=after, d=before) 의 컬럼맵
 */
public record ChangeEvent(
        String sourceTable,
        Op op,
        Map<String, Object> row,
        long seq          // 전역 단조 순번 = Postgres LSN (없으면 카프카 offset fallback)
) {}
