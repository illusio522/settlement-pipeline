-- 7-1 스모크: Spark ↔ Nessie(카탈로그) ↔ MinIO(S3) 삼각연결 검증.
--   네임스페이스·테이블 생성 → append → 조회 → 정리. 성공 = 배관 정상.
CREATE NAMESPACE IF NOT EXISTS nessie.smoke;

CREATE TABLE IF NOT EXISTS nessie.smoke.ping (id BIGINT, name STRING) USING iceberg;

INSERT INTO nessie.smoke.ping VALUES (1, 'hello'), (2, 'iceberg');

SELECT '=== ping rows ===' AS marker;
SELECT * FROM nessie.smoke.ping ORDER BY id;

SELECT '=== snapshots (append 이력) ===' AS marker;
SELECT snapshot_id, operation FROM nessie.smoke.ping.snapshots;

DROP TABLE nessie.smoke.ping;
