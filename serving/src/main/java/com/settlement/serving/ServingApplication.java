package com.settlement.serving;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** 정산 조회 서빙 API — servedb(read-only) 위의 얇은 조회 계층. */
@SpringBootApplication
public class ServingApplication {
    public static void main(String[] args) {
        SpringApplication.run(ServingApplication.class, args);
    }
}
