package com.settlement.normalize.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Debezium 메시지(JSON 문자열) → ChangeEvent 파싱.
 *  - 톰스톤(value=null) → 스킵(Optional.empty)
 *  - op 에 따라 유효 행 선택: DELETE=before, 그 외(c/u/r)=after
 *  - 토픽명(pay.public.payment_card) 끝 segment 로 소스 테이블 식별
 */
@Component
public class CdcParser {

    private final ObjectMapper mapper;

    public CdcParser(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public Optional<ChangeEvent> parse(ConsumerRecord<String, String> rec) {
        String value = rec.value();
        if (value == null) return Optional.empty();   // 톰스톤
        try {
            DebeziumEnvelope env = mapper.readValue(value, DebeziumEnvelope.class);
            Op op = Op.from(env.op());
            JsonNode row = (op == Op.DELETE) ? env.before() : env.after();
            if (row == null || row.isNull()) return Optional.empty();

            @SuppressWarnings("unchecked")
            Map<String, Object> data = mapper.convertValue(row, Map.class);
            // 전역 단조 순번 = source.lsn (DB 전체에서 단조). 없으면 카프카 offset fallback.
            // ★ 카프카 offset은 토픽마다 따로 세므로, 여러 토픽을 합치는 canonical 스트림의
            //   watermark로 부적합 → LSN 사용.
            long seq = (env.source() != null && env.source().hasNonNull("lsn"))
                    ? env.source().get("lsn").asLong()
                    : rec.offset();
            return Optional.of(new ChangeEvent(sourceTable(rec.topic()), op, data, seq));
        } catch (Exception e) {
            throw new RuntimeException(
                    "CDC 파싱 실패 topic=" + rec.topic() + " key=" + rec.key(), e);
        }
    }

    private String sourceTable(String topic) {
        int idx = topic.lastIndexOf('.');
        return idx >= 0 ? topic.substring(idx + 1) : topic;
    }
}
