package com.settlement.normalize.cdc;

/**
 * CDC 메시지 자체가 잘못된 경우(깨진 JSON·구조 이상·필수 필드 누락).
 *  - 같은 메시지를 몇 번 다시 처리해도 결과가 같음 → 재시도 무의미.
 *  - 따라서 error handler에서 non-retryable로 분류해 곧장 DLQ로 보낸다.
 *  - 톰스톤·구독외처럼 "정상적으로 건너뛸" 경우는 예외가 아니라 Optional.empty (이건 던지지 않음).
 */
public class CdcParseException extends RuntimeException {
    public CdcParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
