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
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")     // 서빙 조회 (read-only)
    implementation("org.springframework.boot:spring-boot-starter-validation")   // 입력 검증
    runtimeOnly("org.postgresql:postgresql")                                    // servedb 드라이버
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.test { useJUnitPlatform() }
