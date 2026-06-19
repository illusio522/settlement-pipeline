package com.settlement.normalize.canonical;

import com.settlement.normalize.cdc.Op;

import java.util.Map;

/**
 * 정규화 결과 — 정산이 쓰는 canonical 형태.
 *   table : 대상 canonical 테이블 (payment | payment_method | order_item | cancel)
 *   key   : 멱등 dedup 자연키 (payment=txn_id, payment_method=pm_id,
 *           order_item=order_no:line_no, cancel=cancel_txn_id:order_no:line_no)
 *   op    : 변경 종류 (적재 정책에 사용)
 *   data  : canonical 컬럼맵 (정산 필요분만, 순서 보존 LinkedHashMap)
 */
public record CanonicalRecord(
        String table,
        String key,
        Op op,
        long seq,          // 전역 단조 순번(LSN) — 읽기 dedup·watermark 기준
        Map<String, Object> data
) {}
