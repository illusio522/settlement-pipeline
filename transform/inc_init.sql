-- 3-4: 증분 정산 초기화 (영구 상태 DB)
--   01_schema.sql 로 테이블 생성 후 실행. 데이터는 비움(증분 실행이 채움).
--   watermark = 스트림별 "여기까지 처리한 _seq" (책갈피). 처음엔 -1(아무것도 안 함).

CREATE OR REPLACE TABLE _watermark (stream VARCHAR PRIMARY KEY, last_seq BIGINT);
INSERT INTO _watermark VALUES
  ('payment', -1), ('payment_method', -1), ('order_item', -1), ('cancel', -1);

-- 마스터 (스냅샷, 1회)
INSERT INTO partner
SELECT g, CASE WHEN g=1 THEN 'musinsa-main' ELSE 'partner-'||g END, CASE WHEN g<=50 THEN 1 ELSE 2 END
FROM generate_series(1,100) AS s(g);
INSERT INTO method_fee VALUES
  (1,0.0250,DATE '2020-01-01'),(2,0.0150,DATE '2020-01-01'),
  (3,0.0000,DATE '2020-01-01'),(4,0.0000,DATE '2020-01-01'),(1,0.0230,DATE '2026-06-01');
INSERT INTO mid_map VALUES (1001,1),(1003,1),(1002,2),(1004,2),(1101,1),(1102,2);
