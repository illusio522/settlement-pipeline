package com.settlement.normalize;

import com.settlement.normalize.cdc.CdcParseException;
import com.settlement.normalize.metrics.AssignmentMetrics;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @KafkaListener 컨테이너 팩토리 커스텀.
 *  - 리밸런스 리스너(배정 메트릭) 부착.
 *  - ★ A2: 에러 핸들러(재시도+DLQ). 처리 실패 메시지를 유실하지 않고 <topic>-dlt 로 격리.
 *    · 악성 메시지(CdcParseException) = non-retryable → 재시도 없이 바로 DLQ.
 *    · 일시 오류(쓰기 IO 등) = retryable → 지수 백오프 3회 재시도 후 DLQ.
 *    · DLQ 발행 수는 settlement.dlq.records 카운터로 노출(Grafana).
 */
@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaProperties kafkaProps, MeterRegistry registry,
                                                 @Value("${app.registry-url}") String registryUrl) {
        Counter dlqCounter = Counter.builder("settlement.dlq.records")
                .description("처리 실패로 DLQ(<topic>-dlt)에 보낸 메시지 수")
                .register(registry);

        // ★ 8단계: DLT 발행 템플릿을 값 타입별로 분기.
        //  · 역직렬화 실패(poison) → ErrorHandlingDeserializer가 원본 byte[] 전달 → 그대로 발행
        //  · 리스너 처리 실패 → 값이 GenericRecord → Avro 재직렬화(DLT 스키마 auto-register)
        Map<String, Object> base = kafkaProps.buildProducerProperties(null);
        Map<String, Object> bytesProps = new HashMap<>(base);
        bytesProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        Map<String, Object> avroProps = new HashMap<>(base);
        avroProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
        avroProps.put(SerdeConfig.REGISTRY_URL, registryUrl);
        avroProps.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(bytesProps)));
        templates.put(Object.class, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(avroProps)));

        // 재시도 소진/비재시도 시 → DLT(<topic>.DLT) 발행 + 카운트
        //  파티션 = -1 → 프로듀서가 결정(원본 파티션 N이 DLT 파티션 수보다 크면 발행 실패 → 유실 방지)
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            dlqCounter.increment();
            dlt.accept(record, ex);
        };

        // 일시 오류: 1초 간격으로 최대 3회 재시도 후 DLQ
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // 악성 메시지는 몇 번 돌려도 실패 → 재시도 건너뛰고 바로 DLQ
        handler.addNotRetryableExceptions(CdcParseException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            AssignmentMetrics assignmentMetrics,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);      // Boot 기본 설정 그대로
        factory.getContainerProperties().setConsumerRebalanceListener(assignmentMetrics);
        factory.setCommonErrorHandler(kafkaErrorHandler);          // ★ A2: 재시도+DLQ
        return factory;
    }
}
