plugins {
    java
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.settlement"
version = "0.1.0"

java {
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}

repositories { mavenCentral() }

// 의존성 버전 잠금 (gradle.lockfile) — 갱신은 ./gradlew dependencies --write-locks
dependencyLocking { lockAllConfigurations() }

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-json")   // Jackson(JSON 파싱)
    implementation("org.springframework.kafka:spring-kafka")              // @KafkaListener
    implementation("org.springframework.boot:spring-boot-starter-actuator")     // 메트릭 엔드포인트
    implementation("org.springframework.boot:spring-boot-starter-web")          // /actuator/prometheus 노출용
    implementation("io.micrometer:micrometer-registry-prometheus")              // Prometheus 형식 메트릭
    implementation("io.apicurio:apicurio-registry-serdes-avro-serde:2.4.1.Final") // Avro 역직렬화 (Connect 내장 컨버터와 동일 버전)
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

tasks.test { useJUnitPlatform() }
