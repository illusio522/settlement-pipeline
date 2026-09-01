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
 * 쓰기 시점(B 방식): canonical 레코드를 테이블-파티션별 NDJSON 샤드 파일에 append.
 *  - ★ 병렬화: JSON 직렬화는 락 밖에서, 쓰기는 (테이블,파티션)별 락(holder).
 *  - ★ 샤딩(Phase5): 파일을 Kafka 파티션별로 분리 → <table>.p<partition>.ndjson.
 *    한 파티션=한 스레드(Kafka 보장)라 같은 샤드는 한 스레드만 씀 → 락 경합 0, 진짜 병렬 쓰기.
 *    (단일 토픽 order_item: p0~3을 sink-5~8이 각자 전용 파일에 → 단일락 천장 제거)
 *  - 각 줄에 메타: _op(c/u/d/r), _seq(LSN). 읽기측은 <table>.p*.ndjson 글롭으로 모든 샤드 dedup.
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

    public void write(CanonicalRecord rec, int partition) {
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
        // 2) 쓰기는 (테이블,파티션)별 holder 락 → 같은 샤드는 한 스레드만 → 경합 0
        //    샤드 키 = <table>.p<partition> → 파일 <table>.p<partition>.ndjson
        Holder h = holderFor(rec.table() + ".p" + partition);
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
        // close()는 내부에서 flush 수행 → 실패 = 미flush 버퍼(샤드당 최대 FLUSH_EVERY건) 유실 가능.
        // 삼키면 로그가 정상 종료로 위장하므로 반드시 error로 남긴다.
        int failed = 0;
        for (Map.Entry<String, Holder> e : holders.entrySet()) {
            Holder h = e.getValue();
            synchronized (h) {
                try {
                    h.writer.close();
                } catch (IOException ex) {
                    failed++;
                    log.error("landing close 실패 — 미flush 버퍼 유실 가능 shard={}", e.getKey(), ex);
                }
            }
        }
        if (failed > 0) {
            log.error("landing writers closed ({}개 중 {}개 실패)", holders.size(), failed);
        } else {
            log.info("landing writers closed ({}개)", holders.size());
        }
    }
}
