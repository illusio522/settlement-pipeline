package com.settlement.normalize.cdc;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CdcParser 단위 테스트 — Debezium 봉투(Avro GenericRecord) → ChangeEvent 파싱 규칙 고정.
 *  픽스처는 전부 합성 데이터(주문번호 1001 등 더미 값).
 */
class CdcParserTest {

    private final CdcParser parser = new CdcParser();

    // ---------- Avro 픽스처 스키마 (Debezium 봉투 최소형) ----------

    private static final Schema ROW = SchemaBuilder.record("Row").fields()
            .optionalLong("order_no")
            .optionalLong("amount")
            .endRecord();

    private static final Schema SOURCE = SchemaBuilder.record("Source").fields()
            .optionalLong("lsn")
            .endRecord();

    private static final Schema ENVELOPE = SchemaBuilder.record("Envelope").fields()
            .name("before").type().unionOf().nullType().and().type(ROW).endUnion().nullDefault()
            .name("after").type().unionOf().nullType().and().type(ROW).endUnion().nullDefault()
            .requiredString("op")
            .name("source").type().unionOf().nullType().and().type(SOURCE).endUnion().nullDefault()
            .endRecord();

    private static GenericRecord row(Long orderNo, Long amount) {
        GenericRecord r = new GenericData.Record(ROW);
        r.put("order_no", orderNo);
        r.put("amount", amount);
        return r;
    }

    private static GenericRecord source(Long lsn) {
        GenericRecord s = new GenericData.Record(SOURCE);
        s.put("lsn", lsn);
        return s;
    }

    private static GenericRecord envelope(GenericRecord before, GenericRecord after, String op, GenericRecord src) {
        GenericRecord e = new GenericData.Record(ENVELOPE);
        e.put("before", before);
        e.put("after", after);
        e.put("op", op);
        e.put("source", src);
        return e;
    }

    /** 테스트용 CDC 레코드. 파티션은 파싱에 영향 없으므로 0 고정. */
    private static ConsumerRecord<String, GenericRecord> record(String topic, long offset, GenericRecord value) {
        return new ConsumerRecord<>(topic, 0, offset, "key-1", value);
    }

    // ---------- 행 선택 규칙 (c/u/r = after, d = before) ----------

    @Test
    @DisplayName("op=c 이면 after 행을 반환한다")
    void create_takes_after_row() {
        // given
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", source(42L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 7L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.CREATE);
        assertThat(result.get().row())
                .containsEntry("order_no", 1001L)
                .containsEntry("amount", 5000L);
    }

    @Test
    @DisplayName("op=u 이면 수정 후(after) 행을 반환한다")
    void update_takes_after_row() {
        // given
        GenericRecord value = envelope(row(1001L, 5000L), row(1001L, 7000L), "u", source(43L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 8L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.UPDATE);
        assertThat(result.get().row()).containsEntry("amount", 7000L);
    }

    @Test
    @DisplayName("op=r(스냅샷 read)이면 after 행을 반환한다")
    void snapshot_read_takes_after_row() {
        // given
        GenericRecord value = envelope(null, row(1002L, 3000L), "r", source(10L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 1L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.READ);
        assertThat(result.get().row()).containsEntry("order_no", 1002L);
    }

    @Test
    @DisplayName("op=d 이면 삭제 전(before) 행을 반환한다")
    void delete_takes_before_row() {
        // given
        GenericRecord value = envelope(row(1001L, 5000L), null, "d", source(44L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("order.public.order_item", 9L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().op()).isEqualTo(Op.DELETE);
        assertThat(result.get().row()).containsEntry("order_no", 1001L);
    }

    // ---------- 스킵 규칙 ----------

    @Test
    @DisplayName("톰스톤(value=null)은 스킵한다")
    void tombstone_is_skipped() {
        // given — 삭제 후 카프카가 남기는 key-only 레코드
        GenericRecord value = null;

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 3L, value));

        // then
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("유효 행(after)이 null 이면 스킵한다")
    void explicit_null_row_is_skipped() {
        // given — op=c 인데 after 가 null (비정상이지만 방어)
        GenericRecord value = envelope(null, null, "c", source(45L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 4L, value));

        // then
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("봉투 스키마에 유효 행 필드 자체가 없으면 스킵한다")
    void missing_row_field_is_skipped() {
        // given — after 필드가 아예 없는 스키마 (다른 버전 봉투 방어)
        Schema noAfter = SchemaBuilder.record("Envelope2").fields()
                .name("before").type().unionOf().nullType().and().type(ROW).endUnion().nullDefault()
                .requiredString("op")
                .endRecord();
        GenericRecord value = new GenericData.Record(noAfter);
        value.put("op", "c");

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 5L, value));

        // then
        assertThat(result).isEmpty();
    }

    // ---------- _seq (LSN 우선, offset fallback) ----------

    @Test
    @DisplayName("source.lsn 이 있으면 _seq 로 쓴다 (DB 전역 단조)")
    void seq_uses_lsn_when_present() {
        // given
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", source(171139480L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 7L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(171139480L);
    }

    @Test
    @DisplayName("source 에 lsn 이 없으면 카프카 offset 으로 대체한다")
    void seq_falls_back_to_offset_without_lsn() {
        // given — lsn 필드가 null 인 source
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", source(null));

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 99L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(99L);
    }

    @Test
    @DisplayName("source 자체가 없어도 카프카 offset 으로 대체한다")
    void seq_falls_back_to_offset_without_source() {
        // given
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", null);

        // when
        Optional<ChangeEvent> result = parser.parse(record("pay.public.payment", 123L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().seq()).isEqualTo(123L);
    }

    // ---------- 소스 테이블 식별 (토픽명 마지막 segment) ----------

    @Test
    @DisplayName("토픽명 마지막 점 뒤 segment 가 소스 테이블명이 된다")
    void source_table_is_last_topic_segment() {
        // given
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", source(1L));

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
        GenericRecord value = envelope(null, row(1001L, 5000L), "c", source(1L));

        // when
        Optional<ChangeEvent> result = parser.parse(record("plaintopic", 1L, value));

        // then
        assertThat(result).isPresent();
        assertThat(result.get().sourceTable()).isEqualTo("plaintopic");
    }

    // ---------- 실패 규칙 (봉투 구조 위반 → non-retryable) ----------

    @Test
    @DisplayName("봉투가 아닌 레코드(op 필드 없음)는 CdcParseException 을 던진다 (DLQ 직행용)")
    void broken_envelope_throws_parse_exception() {
        // given — Debezium 봉투가 아닌 임의 레코드
        Schema notEnvelope = SchemaBuilder.record("NotEnvelope").fields()
                .optionalString("whatever")
                .endRecord();
        GenericRecord value = new GenericData.Record(notEnvelope);
        value.put("whatever", "x");

        // when / then
        assertThatThrownBy(() -> parser.parse(record("pay.public.payment", 1L, value)))
                .isInstanceOf(CdcParseException.class)
                .hasMessageContaining("pay.public.payment");
    }
}
