package com.settlement.normalize;

import com.settlement.normalize.metrics.AssignmentMetrics;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * @KafkaListener 컨테이너 팩토리를 커스텀해 리밸런스 리스너(배정 메트릭)를 부착.
 *  - configurer.configure(...)가 spring.kafka.* 설정(concurrency=8, CooperativeSticky, max-poll 등)을 그대로 적용.
 *    → 우리는 그 위에 ConsumerRebalanceListener 만 얹는다(다른 동작 불변).
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            AssignmentMetrics assignmentMetrics) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);      // Boot 기본 설정 그대로
        factory.getContainerProperties().setConsumerRebalanceListener(assignmentMetrics);
        return factory;
    }
}
