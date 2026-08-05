package com.settlement.normalize.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CdcParser 단위 테스트 — Debezium 봉투(JSON) → ChangeEvent 파싱 규칙 고정.
 *  픽스처는 전부 합성 데이터(주문번호 1001 등 더미 값).
 */
class CdcParserTest {

    private final CdcParser parser = new CdcParser(new ObjectMapper());

    /** 테스트용 CDC 레코드. 파티션은 파싱에 영향 없으므로 0 고정. */
    private static ConsumerRecord<String, String> record(String topic, long offset, String value) {
        return new ConsumerRecord<>(topic, 0, offset, "key-1", value);
    }

    // ---------- 행 선택 규칙 (c/u/r = after, d = before) ----------

    @Test
    @DisplayName("op=c 이면 after 행을 반환한다")
    void create_takes_after_row() {
        // given
        String value = """
                {"before":null,
                 "after":{"order_no":1001,"amount":5000},
                 "op":"c",
                 "source":{"lsn":42}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 7L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.CREATE);
        assertThat(result.get().row())
                .containsEntry("order_no", 1001)
                .containsEntry("amount", 5000);
    }

    @Test
    @DisplayName("op=u 이면 수정 후(after) 행을 반환한다")
    void update_takes_after_row() {
        // given
        String value = """
                {"before":{"order_no":1001,"amount":5000},
                 "after":{"order_no":1001,"amount":7000},
                 "op":"u",
                 "source":{"lsn":43}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 8L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.UPDATE);
        assertThat(result.get().row()).containsEntry("amount", 7000);
    }

    @Test
    @DisplayName("op=r(스냅샷 read)이면 after 행을 반환한다")
    void snapshot_read_takes_after_row() {
        // given
        String value = """
                {"before":null,
                 "after":{"order_no":1002,"amount":3000},
                 "op":"r",
                 "source":{"lsn":10}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 0L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.READ);
        assertThat(result.get().row()).containsEntry("order_no", 1002);
    }

    @Test
    @DisplayName("op=d 이면 삭제된 행(before)을 반환한다")
    void delete_takes_before_row() {
        // given
        String value = """
                {"before":{"order_no":1003,"amount":9000},
                 "after":null,
                 "op":"d",
                 "source":{"lsn":77}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 9L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.DELETE);
        assertThat(result.get().row()).containsEntry("order_no", 1003);
    }

    // ---------- 건너뛰기 (예외가 아니라 Optional.empty) ----------

    @Test
    @DisplayName("톰스톤(value=null)은 예외 없이 건너뛴다")
    void tombstone_is_skipped() {
        // given
        ConsumerRecord<String, String> tombstone = record("pay.public.payment", 1L, null);

        // when
        Optional<ChangeEvent> result = parser.parse(tombstone);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("선택된 행이 JSON null이면 건너뛴다")
    void explicit_null_row_is_skipped() {
        // given — op=c 인데 after 가 명시적 null
        String value = """
                {"before":null,"after":null,"op":"c","source":{"lsn":50}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 2L, value));

        // then
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("선택된 행 키 자체가 없으면 건너뛴다")
    void missing_row_field_is_skipped() {
        // given — after 키가 아예 없는 봉투
        String value = """
                {"op":"c","source":{"lsn":51}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 3L, value));

        // then
        assertThat(result).isEmpty();
    }

    // ---------- seq 결정 (source.lsn 우선, 없으면 카프카 offset) ----------

    @Test
    @DisplayName("source.lsn이 있으면 seq는 LSN을 쓴다")
    void seq_uses_lsn_when_present() {
        // given — LSN(999)과 offset(5)을 다르게 둬서 어느 쪽을 썼는지 구분
        String value = """
                {"after":{"payment_no":2001},"op":"c","source":{"lsn":999}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 5L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(999L);
    }

    @Test
    @DisplayName("source에 lsn이 없으면 seq는 카프카 offset으로 폴백한다")
    void seq_falls_back_to_offset_without_lsn() {
        // given — source는 있지만 lsn 없음
        String value = """
                {"after":{"payment_no":2002},"op":"c","source":{"db":"paydb"}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 5L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(5L);
    }

    @Test
    @DisplayName("source 자체가 없으면 seq는 카프카 offset으로 폴백한다")
    void seq_falls_back_to_offset_without_source() {
        // given
        String value = """
                {"after":{"payment_no":2003},"op":"c"}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 12L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(12L);
    }

    // ---------- 소스 테이블명 추출 (토픽 마지막 세그먼트) ----------

    @Test
    @DisplayName("토픽의 마지막 세그먼트를 소스 테이블명으로 쓴다")
    void source_table_is_last_topic_segment() {
        // given
        String value = """
                {"after":{"payment_no":2004},"op":"c","source":{"lsn":60}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment_card", 1L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().sourceTable()).isEqualTo("payment_card");
    }

    @Test
    @DisplayName("점이 없는 토픽명은 그대로 테이블명이 된다")
    void source_table_without_dot_is_topic_itself() {
        // given
        String value = """
                {"after":{"payment_no":2005},"op":"c","source":{"lsn":61}}
                """;

        // when
        Optional<ChangeEvent> result = parser.parse(record("payment", 1L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().sourceTable()).isEqualTo("payment");
    }

    // ---------- 실패 (재시도 무의미 → DLQ 직행) ----------

    @Test
    @DisplayName("깨진 JSON은 CdcParseException으로 던진다")
    void broken_json_throws_parse_exception() {
        // given
        ConsumerRecord<String, String> broken = record("pay.public.payment", 3L, "{\"after\":");

        // when / then — 토픽이 메시지에 남아야 DLQ에서 출처를 추적할 수 있다
        assertThatThrownBy(() -> parser.parse(broken))
                .isInstanceOf(CdcParseException.class)
                .hasMessageContaining("pay.public.payment");
    }
}
