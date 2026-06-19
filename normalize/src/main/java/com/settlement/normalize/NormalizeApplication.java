package com.settlement.normalize;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 정산 정규화 싱크 — Kafka CDC 토픽을 읽어 canonical 형태로 NDJSON 랜딩에 적재.
 * (수단별 4원장 UNION + 이름→코드 정규화 + 멱등 적재는 작업 3~4에서 추가)
 */
@SpringBootApplication
public class NormalizeApplication {
    public static void main(String[] args) {
        SpringApplication.run(NormalizeApplication.class, args);
    }
}
