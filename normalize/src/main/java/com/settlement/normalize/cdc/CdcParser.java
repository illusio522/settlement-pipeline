package com.settlement.normalize.cdc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Debezium 메시지(Avro GenericRecord) → ChangeEvent 파싱.
 *  - 톰스톤(value=null) → 스킵(Optional.empty)
 *  - op 에 따라 유효 행 선택: DELETE=before, 그 외(c/u/r)=after
 *  - 토픽명(pay.public.payment_card) 끝 segment 로 소스 테이블 식별
 *  - 스키마는 레지스트리(Apicurio)가 관리 — 역직렬화 시점에 이미 구조 검증됨.
 */
@Component
public class CdcParser {

    public Optional<ChangeEvent> parse(ConsumerRecord<String, GenericRecord> rec) {
        GenericRecord env = rec.value();
        if (env == null) return Optional.empty();   // 톰스톤
        try {
            Op op = Op.from(asString(env.get("op")));   // op 필드 없음 = 봉투 아님 → 예외
            GenericRecord row = (GenericRecord) field(env, (op == Op.DELETE) ? "before" : "after");
            if (row == null) return Optional.empty();

            Map<String, Object> data = toMap(row);
            // 전역 단조 순번 = source.lsn (DB 전체에서 단조). 없으면 카프카 offset fallback.
            // ★ 카프카 offset은 토픽마다 따로 세므로, 여러 토픽을 합치는 canonical 스트림의
            //   watermark로 부적합 → LSN 사용.
            GenericRecord source = (GenericRecord) field(env, "source");
            Object lsn = (source != null) ? field(source, "lsn") : null;
            long seq = (lsn instanceof Number n) ? n.longValue() : rec.offset();
            return Optional.of(new ChangeEvent(sourceTable(rec.topic()), op, data, seq));
        } catch (Exception e) {
            // 메시지 자체가 잘못됨 → 재시도 무의미 → non-retryable(DLQ 직행)
            throw new CdcParseException(
                    "CDC 파싱 실패 topic=" + rec.topic() + " key=" + rec.key(), e);
        }
    }

    /** 스키마에 없는 필드는 null 취급 (버전 다른 스키마와도 안전). */
    private static Object field(GenericRecord r, String name) {
        return r.getSchema().getField(name) != null ? r.get(name) : null;
    }

    /** Avro 행 → 컬럼맵. Avro 전용 타입(Utf8 등)을 JSON 직렬화 가능한 표준 타입으로 변환. */
    private static Map<String, Object> toMap(GenericRecord row) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (Schema.Field f : row.getSchema().getFields()) {
            m.put(f.name(), plain(row.get(f.name())));
        }
        return m;
    }

    private static Object plain(Object v) {
        if (v instanceof Utf8 u) return u.toString();
        if (v instanceof GenericRecord r) return toMap(r);
        if (v instanceof CharSequence c) return c.toString();   // enum symbol 등
        return v;   // Long·Integer·Double·Boolean·null 은 그대로
    }

    private static String asString(Object v) {
        return v == null ? null : v.toString();
    }

    private String sourceTable(String topic) {
        int idx = topic.lastIndexOf('.');
        return idx >= 0 ? topic.substring(idx + 1) : topic;
    }
}
