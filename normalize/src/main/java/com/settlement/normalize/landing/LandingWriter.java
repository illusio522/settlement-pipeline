package com.settlement.normalize.landing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.settlement.normalize.canonical.CanonicalRecord;
import com.settlement.normalize.cdc.Op;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 쓰기 시점(B 방식): canonical 레코드를 테이블별 NDJSON 파일에 append.
 *  - ★ 병렬화: JSON 직렬화는 락 밖에서, 쓰기는 테이블별 락(holder) → 서로 다른 테이블 동시 쓰기 허용.
 *    (전역 synchronized → 테이블별 lock 으로 변경, 컨슈머 concurrency와 함께 병렬 처리량 ↑)
 *  - 각 줄에 메타: _op(c/u/d/r), _seq(LSN). 파일: <landing-dir>/<table>.ndjson
 */
@Component
public class LandingWriter {

    private static final Logger log = LoggerFactory.getLogger(LandingWriter.class);
    private static final int FLUSH_EVERY = 2000;

    private final ObjectMapper mapper;
    private final Path dir;
    private final Map<String, Holder> holders = new ConcurrentHashMap<>();

    /** 테이블별 파일 + flush 카운터 (이 객체에 락을 걸어 같은 파일 쓰기만 직렬화). */
    private static final class Holder {
        final BufferedWriter writer;
        long sinceFlush = 0;
        Holder(BufferedWriter w) { this.writer = w; }
    }

    public LandingWriter(ObjectMapper mapper, @Value("${app.landing-dir}") String landingDir) {
        this.mapper = mapper;
        this.dir = Paths.get(landingDir);
    }

    public void write(CanonicalRecord rec) {
        // 1) 직렬화는 락 밖에서 (CPU 작업 병렬)
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("_op", opCode(rec.op()));
        line.put("_seq", rec.seq());
        line.putAll(rec.data());
        String json;
        try {
            json = mapper.writeValueAsString(line);
        } catch (IOException e) {
            throw new RuntimeException("직렬화 실패 table=" + rec.table(), e);
        }
        // 2) 쓰기는 테이블별 holder 락만 (다른 테이블끼리 병렬)
        Holder h = holderFor(rec.table());
        synchronized (h) {
            try {
                h.writer.write(json);
                h.writer.newLine();
                if (++h.sinceFlush >= FLUSH_EVERY) { h.writer.flush(); h.sinceFlush = 0; }
            } catch (IOException e) {
                throw new RuntimeException("랜딩 쓰기 실패 table=" + rec.table(), e);
            }
        }
    }

    private Holder holderFor(String table) {
        return holders.computeIfAbsent(table, t -> {
            try {
                Files.createDirectories(dir);
                Path file = dir.resolve(t + ".ndjson");
                log.info("landing open: {}", file.toAbsolutePath());
                return new Holder(Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
            } catch (IOException e) {
                throw new RuntimeException("랜딩 파일 열기 실패 " + t, e);
            }
        });
    }

    private static String opCode(Op op) {
        return switch (op) {
            case CREATE -> "c";
            case UPDATE -> "u";
            case DELETE -> "d";
            case READ   -> "r";
            default     -> "?";
        };
    }

    @PreDestroy
    public void close() {
        holders.forEach((t, h) -> {
            synchronized (h) { try { h.writer.close(); } catch (IOException ignore) { } }
        });
        log.info("landing writers closed ({}개)", holders.size());
    }
}
