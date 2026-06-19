package com.settlement.normalize.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 스레드(컨슈머)별 토픽-파티션 배정을 Prometheus 게이지로 노출.
 *  - 리밸런스 콜백에서 배정/해제 시점을 잡아 게이지 갱신(배정=1, 해제=0).
 *  - 메트릭: settlement_consumer_assignment{client_id, topic, partition} = 1|0
 *  - Grafana Table/State-timeline 으로 "어느 스레드가 어느 토픽-파티션을 들었나" + 리밸런스 변동 시각화.
 *  ※ 카프카 기본 메트릭엔 client_id별 "개수"만 있고 토픽 매핑이 없어, 앱이 직접 라벨을 달아 내보낸다.
 */
@Component
public class AssignmentMetrics implements ConsumerAwareRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(AssignmentMetrics.class);

    private final MeterRegistry registry;
    /** (client|topic|partition) → 상태값(1/0). 같은 키는 게이지 1회만 등록하고 값만 갱신. */
    private final Map<String, AtomicInteger> gauges = new ConcurrentHashMap<>();

    public AssignmentMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String clientId = clientIdOf(consumer);
        for (TopicPartition tp : partitions) {
            state(clientId, tp).set(1);
        }
        log.info("배정 [{}] +{}개", clientId, partitions.size());
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        String clientId = clientIdOf(consumer);
        for (TopicPartition tp : partitions) {
            state(clientId, tp).set(0);
        }
    }

    /** 게이지를 (client,topic,partition)당 1회 등록하고 그 상태 AtomicInteger 반환. */
    private AtomicInteger state(String clientId, TopicPartition tp) {
        String key = clientId + "|" + tp.topic() + "|" + tp.partition();
        return gauges.computeIfAbsent(key, k -> {
            AtomicInteger v = new AtomicInteger(0);
            Gauge.builder("settlement.consumer.assignment", v, AtomicInteger::get)
                    .description("스레드(client_id)별 토픽-파티션 배정 여부(1=배정,0=해제)")
                    .tag("client_id", clientId)
                    .tag("topic", tp.topic())
                    .tag("partition", String.valueOf(tp.partition()))
                    .tag("tp", shortTopic(tp.topic()) + "-" + tp.partition())  // 합친 표기: cancel-0
                    .register(registry);
            return v;
        });
    }

    /** "order.public.cancel" → "cancel" (마지막 '.' 뒤만, 합친 표기용). */
    private static String shortTopic(String topic) {
        int i = topic.lastIndexOf('.');
        return i >= 0 ? topic.substring(i + 1) : topic;
    }

    /** KafkaConsumer는 client.id 공개 getter가 없어, 클라이언트 메트릭 태그에서 추출. */
    private static String clientIdOf(Consumer<?, ?> consumer) {
        return consumer.metrics().keySet().stream()
                .map(m -> m.tags().get("client-id"))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("unknown");
    }
}
