package com.settlement.normalize.cdc;

/** Debezium op 코드 → 의미 있는 enum. */
public enum Op {
    CREATE, UPDATE, DELETE, READ, UNKNOWN;

    public static Op from(String op) {
        if (op == null) return UNKNOWN;
        return switch (op) {
            case "c" -> CREATE;
            case "u" -> UPDATE;
            case "d" -> DELETE;
            case "r" -> READ;   // 스냅샷 read
            default -> UNKNOWN;
        };
    }
}
