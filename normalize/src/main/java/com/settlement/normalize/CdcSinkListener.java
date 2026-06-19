package com.settlement.normalize;

import com.settlement.normalize.canonical.Normalizer;
import com.settlement.normalize.cdc.CdcParser;
import com.settlement.normalize.landing.LandingWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 싱크 컨슈머: 카프카 CDC → 파싱 → 정규화(canonical) → NDJSON 랜딩 적재.
 *  파싱/정규화에서 걸러진 것(톰스톤·구독외)은 자연 스킵(Optional 빈값).
 *  _seq = 카프카 오프셋(파티션 내 단조증가) → 읽기 시점 dedup의 최신 판별 기준.
 */
@Component
public class CdcSinkListener {

    private static final Logger log = LoggerFactory.getLogger(CdcSinkListener.class);

    private final CdcParser parser;
    private final Normalizer normalizer;
    private final LandingWriter writer;
    private final AtomicLong landed = new AtomicLong();
    private final Counter landedCounter;   // ★ Micrometer: 랜딩 적재 건수 (Prometheus 노출)

    public CdcSinkListener(CdcParser parser, Normalizer normalizer, LandingWriter writer,
                           MeterRegistry registry) {
        this.parser = parser;
        this.normalizer = normalizer;
        this.writer = writer;
        this.landedCounter = Counter.builder("settlement.landed.records")
                .description("랜딩에 적재된 canonical 레코드 수")
                .register(registry);
    }

    @KafkaListener(topicPattern = "${app.topics-pattern}")
    public void onMessage(ConsumerRecord<String, String> rec) {
        parser.parse(rec)
                .flatMap(normalizer::normalize)
                .ifPresent(c -> {
                    writer.write(c, rec.partition());   // 파티션별 샤드 파일로(충돌0). _seq=LSN
                    landedCounter.increment();   // ★ 적재율 메트릭 (rate()로 msg/s 그래프)
                    long n = landed.incrementAndGet();
                    if (n % 10000 == 0) log.info("landed {} records", n);
                });
    }
}
