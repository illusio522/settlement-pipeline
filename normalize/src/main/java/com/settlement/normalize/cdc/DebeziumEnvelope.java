package com.settlement.normalize.cdc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Debezium 표준 change event 봉투 (schemas.enable=false → payload 직접 노출).
 *   before : 변경 전 행 (c=null, u=수정전, d=삭제된행)
 *   after  : 변경 후 행 (c=새행, u=수정후, d=null)
 *   op     : c=create u=update d=delete r=read(snapshot)
 *   source : 출처 메타(db·table·lsn 등) — 현재 직접 사용 안 함
 * 모르는 필드(transaction, ts_us 등)는 무시.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DebeziumEnvelope(
        JsonNode before,
        JsonNode after,
        String op,
        JsonNode source,
        Long ts_ms
) {}
