-- 7-2d 스냅샷/타임트래블 실증: 2배치 append → 스냅샷 2개 (전체교체면 불가능한 것)
--   demo 테이블에 cancel 을 절반씩 나눠 append. 이후 bash가 스냅샷ID로 VERSION AS OF 조회.
DROP TABLE IF EXISTS nessie.bronze.appenddemo;

-- 배치1: order_no 짝수 절반
CREATE TABLE nessie.bronze.appenddemo USING iceberg AS
SELECT * FROM nessie.silver.cancel WHERE order_no % 2 = 0;

-- 배치2: 홀수 나머지 (★ append — 배치1 위에 추가)
INSERT INTO nessie.bronze.appenddemo
SELECT * FROM nessie.silver.cancel WHERE order_no % 2 = 1;

SELECT '=== batch counts ===' AS marker;
SELECT (SELECT COUNT(*) FROM nessie.silver.cancel WHERE order_no % 2 = 0) AS batch1,
       (SELECT COUNT(*) FROM nessie.silver.cancel WHERE order_no % 2 = 1) AS batch2,
       (SELECT COUNT(*) FROM nessie.bronze.appenddemo) AS total_now;

SELECT '=== snapshots (2개 기대: 배치1, 배치1+2) ===' AS marker;
SELECT snapshot_id, operation FROM nessie.bronze.appenddemo.snapshots ORDER BY committed_at;
