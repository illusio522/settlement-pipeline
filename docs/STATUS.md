# 작업 현재 상태 (재개용)

> 컴팩션/세션 재시작 후 이 파일부터 읽고 이어서 진행.
> 프로젝트: `~/Private/Workspace/settlement-pipeline` — 대규모 파트너 정산 파이프라인 (로컬 학습/검증).

## 진행 방식 (★ 작업 프로토콜 — 항상 준수)

- **설명 → 실행 → 검증** 순서: 각 단계/하위 단계 시작 전에 **무엇을·왜·어떻게** 할지 상세 설명 후 실행한다. (학습 목적 프로젝트 — 결과만이 아니라 과정의 원리 이해가 목표)
- 검증은 실측 증거(체크섬·쿼리 결과)로 제시. 단계별 검증 통과 후 다음으로.
- git commit/push 금지(분석·검증 작업 중), 사용자 요청 시에만.

## 지금 어디까지

**전 단계 구현·검증 완료 — CDC→레이크→정산 파이프라인 풀 E2E 관통 (2026-07-02)**

- 완료: 1)DuckDB 로직검증 / 2)실시간수집(Postgres+Debezium+Kafka+컨슈머) / 3)레이크+dbt증분 / 4)성능·병렬화 / 5-1)쓰기 샤딩 / App-2 DLQ / App-3 dbt incremental / 6·7)Nessie+Spark 메달리온
- 품질·보안 점검 완료(2026-08-31~09-01): 포트 루프백·자격증명 `infra/.env` 분리·kafka 볼륨·CdcParser 테스트 13종·실존 브랜드명 제거·LandingWriter close 예외 삼킴 수정(A10)·공급망 고정(A03: 이미지 태그·sha256·gradle.lockfile)
- 컴퓨트: DuckDB 도커 이미지 `settlement-duckdb:1.5.3`
- 실행: `bash scripts/run-step1.sh` (01→02→03→04 SQL 파이프 → `data/settlement.duckdb`)
- 조회: `bash scripts/q.sh "SQL"` / 대화형 `bash scripts/duckdb-shell.sh`

## ⭐ 즉시 다음 액션

1. `docs/data-flow.html` 레이크 단계 분리 WIP — 렌더 확인 후 커밋만 남음
2. 다이어그램 2종(`system-architecture.html`·`aws-architecture.html`) — v1 상태, 재작성 필요
3. ~~학습 후보(엔드피처): Phase 8 Avro/SchemaReg → 9 Airflow → 10 서빙 API~~ **전부 완료(2026-09-01)** — 로드맵 완주
4. 미결 소항목: Phase 5 병목 국소화 측정, DLQ retryable 경로 라이브 검증, dbt settlement_event incremental화

### 멱등성 vs 재현성 (이번에 정리)
- **멱등성**(원장 고정, 정산만 재실행 → 동일) = ✅. `03_settlement.sql`만 재실행 시 agg/event 체크섬 비트 동일. ※ run-step1.sh는 원장까지 새로 만드니 멱등성 검증용 아님.
- **재현성**(시드 재생성 시 같은 fake 데이터) = ✅. DuckDB random() 2종 비결정을 차단:
  - ① 가변 호출(한 행서 random 호출수가 분기마다 다름): `partner_id`·`cancel kind` → 결정적 `hash(키)`로 교체. threads=1로도 안 잡힘.
  - ② 고정 호출이나 저장테이블 morsel 병렬 스캔서 흔들림: `payment_method` 등 → 시드 구간 `SET threads TO 1`로 차단(03서 `RESET threads`).
  - 검증: 풀파이프 2회 oi/pay/cancel/pm/event/agg 6개 체크섬 전부 동일.

## v3에서 구현·검증된 것 (전부 통과)

- **복합결제**: payment(헤더) + payment_method(결제수단별). 결제수단 4종 — 주(CARD·MONEY) + 보조(POINT·COUPON)
- **method_type 마스터**: 주결제수단 확대 = 행 추가만 (예 `(5,'BANK','MAIN',TRUE)`)
- **주결제수단 동시 사용**(카드+머니 분할결제) 지원
- **결제수단별 정산 명세(백데이터)**: settlement_event를 **(라인 × 결제수단)** 교차배분 / settlement_agg에 method 포함
- **차등 수수료**: `method_fee`(결제수단별, as-of) — 카드 2.5%(6/1~2.3%)·머니 1.5%·포인트/쿠폰 0%
- **부분취소**: `cancel`(취소 라인 명세) → 셀러 정산 차감 (전체 2,475 / 부분 2,536)
- **돈 보존 대사**(recon_result): 주문총액=집계 diff=0
- **주결제수단 MID 대사**(recon_card): 카드·머니 MID별=PG파일 diff=0
- **freeze**: 거래시점 수수료 박제(불변 원장), 멱등(TRUNCATE+재생성)

## 확정 설계 결정

- **개발 스택: Java 21 LTS · Spring Boot 3.x · JPA** — 앱(싱크 컨슈머·오케스트레이션·서빙)은 이 스택. 쿼리 로직은 SQL/dbt. JPA는 연관관계 대신 ID 참조 패턴(물리 FK 없음과 정합).

- 결제 원장(payment)엔 partner 없음 → partner는 order_item에서. 정산 = 결제 ⋈ 주문 조인+배분
- 셀러 정산 = **상품가(item) 기준** (결제수단 무관 총액), 단 **백데이터·수수료는 결제수단별**
- 보조결제수단(포인트·쿠폰) = 플랫폼 부담(수수료 0), 정산 명세엔 표기
- **정산 ≠ 환불** — 환불(자금 정책·대사)은 정산 밖, 안 다룸 (취소는 셀러 정산 차감만)
- 수집: 결제·주문 = CDC / 마스터 = 스냅샷 (실 구현은 2단계)
- 도메인 경계(결제/주문/정산 DB 분리)는 2단계에서

## 파일 상태

- `transform/01_schema.sql` `generator/02_seed.sql` `transform/03_settlement.sql` `transform/04_checks.sql` = **v3** (현행)
- `scripts/run-step1.sh` `q.sh` `duckdb-shell.sh` `infra/duckdb/Dockerfile`
- 다이어그램: `architecture.html`(✅ 2026-07-02 인터랙티브 현행화) · `data-flow.html`(✅ 신규 — 단계별 실데이터 예시) · `system-architecture.html` `aws-architecture.html`(※ 여전히 v1 — 재작성 필요)
- 문서: `DESIGN.md SCHEMA.md DATA-LAYOUT.md DOMAIN-NOTES.md ROADMAP.md ENVIRONMENT.md` (v3 동기화 완료) · `ERD.md`(소스 Postgres + canonical DuckDB, Mermaid — 2026-06-12) · `KAFKA-CONSUMER.md`(파티션·스레드·리밸런스 트리거/타이밍·배분전략(assignor)·장애복구·dedup — 2026-06-18)
  ※ 주의: SCHEMA.md의 소스 테이블 기술은 DuckDB canonical 기준 — **Postgres 소스의 현실형 구조(수단별 4원장)는 ERD.md·infra/postgres/init.sql이 정본**

## 알려진 이슈/주의

- DuckDB 1.5.3: 윈도우 함수 2개를 UNION 위에 겹치면 `unordered_map::at` 크래시 → **윈도우마다 TEMP 테이블로 분리**해 회피(03에 적용됨)
- DuckDB 예약어 `rows` — 컬럼 alias 금지
- DBeaver와 도커 DuckDB 동시 쓰기 금지(단일 writer) → 배치 돌릴 땐 DBeaver 끊기

## 2단계 진행 상황 (실시간 수집)

- **2-1 Postgres ✅ 완료(2026-06-12)**: `infra/postgres/init.sql` — 도메인 분리 paydb/orderdb (커넥터 2개 전제).
  - ★ 소스는 **현실형 구조**: paydb = `payment`(마스터 원장, 총액) + **수단별 원장 4개**(`payment_card/money/point/coupon`, txn_id 논리 FK) — 사용자 지정. orderdb = `order_item`(product_id·qty·unit_price·discount 등 풍부) + `cancel`(reason_code).
  - ★ **물리 FK 전면 미사용(논리 FK only)** — 사용자 결정(JPA·락경합·샤딩). 무결성 = 쓰기 시 앱 Tx + 사후 정산 대사. 인덱스는 유지.
  - 함의: 2-4 싱크에서 **수단별 4원장 UNION + 정규화 → canonical payment_method(DuckDB)** projection. 정산 배치(03)는 무변경. Σ수단=헤더·고아 무결성은 대사가 검증.
  - publication: `dbz_pay`(5테이블)·`dbz_order`(2테이블). wal_level=logical. 검증: FK 0개·고아 INSERT 허용 확인·publication 5+2.
- **2-2 Kafka(KRaft) ✅ 완료(2026-06-12)**: `sp-kafka`(apache/kafka 3.8, broker+controller 겸직) + `sp-kafka-ui`(:8080). 검증: 토픽 생성→produce/consume 3건 왕복→삭제, 호스트 리스너 29092 열림(자바 앱용).
- **2-3 Debezium ✅ 완료(2026-06-15)**: `sp-connect`(debezium/connect **2.7.3.Final** — 주의: `2.7` 태그는 없음). 커넥터 2개 등록(`infra/debezium/pay-connector.json`·`order-connector.json`, pgoutput·publication·slot). 둘 다 RUNNING, slot active=t.
  - 검증: paydb/orderdb INSERT→토픽 자동생성·이벤트 도착(op=c, after 전체). DELETE→op=d(before)+톰스톤(null) 확인. 시험데이터 정리 완료.
  - ★ 토픽은 **데이터 들어간 테이블만** 자동생성 → 현재 업무토픽 2개(payment·order_item)뿐. 나머지 5개(card/money/point/coupon·cancel)는 2-5 전체적재 시 생겨 7개 완성. 커넥터/slot은 5+2 전체 구독 중.
- **2-4 싱크 ✅ 완료(2026-06-16)**: `normalize/` Spring Boot3+Spring Kafka 컨슈머(Java21, Kotlin DSL). 구조: CdcSinkListener→CdcParser(Debezium envelope)→Normalizer(canonical)→LandingWriter(NDJSON).
  - 정규화: 수단별 4원장(card/money/point/coupon)→단일 payment_method+method코드(1~4), 보조 mid=null. payment/order_item/cancel는 정산 필요컬럼만 projection.
  - ★ 멱등 = **B방식(읽기 시점 dedup)**: 컨슈머 무상태로 NDJSON append(`_op`·`_seq`=카프카오프셋), 배치가 읽을때 `key별 _seq 최대 & _op<>'d'`로 dedup. 실증: 재처리로 24줄 쌓인 payment_method→dedup 4행(중복 제거+삭제 반영).
  - 랜딩: `data/landing/<table>.ndjson` (.duckdb 안 건드림 → 단일writer·버전 충돌 회피, 3단계 MinIO/Iceberg의 미니어처).
  - ⚠️ event_time = epoch밀리초(Long)로 적재됨 → 2-5 DuckDB 읽기서 `epoch_ms()`로 TIMESTAMP 변환 필요.
- **2-5 E2E ✅ 완료(2026-06-17) — 2단계 전체 완료**: 1단계 canonical→Postgres 현실형 적재(`scripts/e2e-load-postgres.sh`, payment_method 4원장 분리)→CDC→Kafka(58만건)→컨슈머→랜딩→읽기 dedup으로 canonical 재구성(`transform/e2e_load.sql`)→03 정산(`scripts/run-e2e.sh`)→`data/e2e.duckdb`.
  - ★ 증명: **5종 체크섬(payment·payment_method·order_item·cancel·settlement_agg) 전부 1단계와 비트 동일.** 고아 0, 돈보존·MID대사 diff=0. → 파이프라인 무손실·무변형 운반 증명.
  - 검증 방법 문서: `docs/VERIFICATION.md` (단계별 테스트 케이스·근거).
- 실행: `cd infra && docker compose up -d postgres` (kafka 등은 단계별로)

## 3단계 진행 상황 (레이크 + 증분 멱등)

- **3-1 MinIO 레이크 ✅ 완료(2026-06-17)**: `sp-minio` 기동, 버킷 `lake`. DuckDB httpfs로 s3:// 접근(endpoint=sp-minio:9000, path-style, ssl off, 자격증명은 infra/.env).
  - `scripts/landing-to-minio.sh`: 로컬 랜딩 NDJSON → MinIO Parquet(`s3://lake/landing/<table>.parquet`, _op·_seq 보존).
  - `transform/e2e_load_lake.sql` + `scripts/run-e2e-lake.sh`: MinIO Parquet 읽어 dedup→canonical→정산 → `data/e2e_lake.duckdb`. DuckDB 컨테이너 `--network settlement-pipeline_default`로 sp-minio 접근.
  - ★ 검증: 레이크 기준 5종 체크섬 전부 1단계 동일. + 삭제 전파 실증(데모 900001이 DELETE 이벤트→랜딩 d→dedup 떨굼).
- **3-2 Iceberg ✅ 완료(2026-06-17)**: DuckDB iceberg 확장으로 MinIO에 Iceberg 테이블 쓰기/읽기(`COPY FORMAT ICEBERG`, 카탈로그 없이 파일기반). `build-iceberg.sh`·`e2e_load_iceberg.sql`·`run-e2e-iceberg.sh`. 검증: Iceberg 기준 정산 5종 체크섬 1단계 동일.
  - ★ 학습 포인트: DuckDB COPY는 **전체교체**(append 불가) → 진짜 증분 append는 REST 카탈로그(Nessie)+PyIceberg/Spark 필요. (운영 확장 시 학습 대상)
- **3-4 증분 멱등 ✅ 완료(2026-06-17)**: 랜딩+watermark 방식. `inc_init.sql`·`inc_run.sql`·`inc-init.sh`·`inc-run.sh`, 영구상태 `data/incremental.duckdb`.
  - watermark = 스트림별 "처리한 _seq"(책갈피). `_seq > watermark`만 읽어 영향주문 재정산·event 누적·agg 재계산·watermark 갱신.
  - ★★ 핵심 버그&수정: `_seq`를 카프카 **offset**으로 했더니 깨짐 — canonical payment_method는 **4개 토픽**(card/money/point/coupon)에서 합류하는데 offset은 토픽마다 따로 셈 → 머니 offset이 watermark보다 작아 누락. **수정: `_seq` = Postgres `source.lsn`**(DB 전역 단조). 컨슈머 CdcParser서 lsn 추출. → 실무 CDC 증분이 LSN으로 watermark하는 이유를 실측으로 체득.
  - 검증: 증분(새것만)=전량 재처리 체크섬 동일 / 멱등(0건 시 무변화) / 부분취소 처리 / 800004 카드+머니 정상.
- **3-3 dbt ✅ 완료(2026-06-18)**: `infra/dbt/Dockerfile`(python3.11+dbt-duckdb 1.11, 호스트 파이썬 3.14 회피), `dbt/` 프로젝트. `scripts/run-dbt.sh` (run/test/build).
  - 모델 15개: masters(partner·method_fee·mid_map) / staging(랜딩 dedup 4) / intermediate(int_pay_line·alloc·order_method·detail — 03의 단계별 TEMP를 ref() DAG로) / marts(settlement_event freeze·settlement_agg).
  - ★ dbt가 ref()로 의존순서 자동계산·병렬실행. 결과 settlement_agg 체크섬 = 손작성 SQL 동일.
  - 테스트(`schema.yml`) 15개 PASS: unique(event_id·txn_id·pm_id)·not_null·accepted_values(method 1~4)·**relationships(partner_id→partner = 고아검증, 물리FK 대체)**. `dbt build`=run+test.
  - 미적용: settlement_event를 incremental materialized로 (3-4의 watermark를 dbt 방식으로) — 추후.

## 4단계 (성능·스큐) 진행

- **4-A 단일노드 성능 측정 ✅ 완료(2026-06-18)**: `bench/bench_seed.sql`·`bench/run-bench.sh`. canonical 직접생성(CDC 미경유)→03 정산 계측.
  - 결과: 100k 정산1.7s / 1M 5.6s / 10M 57s(5,116만 이벤트). ~90만 ev/s 수렴, 선형 이하. 1억 외삽 2~9.5분.
  - ★ 크래시: 풀병렬(스레드10)+대량서 SIGSEGV(exit139, 경쟁조건). 해결=`SET threads TO 6; memory_limit='24GB'`(병렬↓·디스크스필). 속도 손해 거의 없이 안정.
  - 결론: 단일노드는 1000만까진 빠르나 대량서 수동튜닝·불안정 → 운영은 분산(4-B Spark) 필요 실증. 상세 `docs/BENCHMARK.md`.
- **4-A(E2E) 전구간 측정 ✅**: 100k주문=58만메시지. ①PG적재6s ②CDC→Kafka1s ③컨슈머랜딩**96s(병목 90%)** ④정산3s. → 병목은 엔진 아니라 **자바 컨슈머**. 결과 체크섬 정답지 동일.
- **4-A(병렬화) ✅ 완료 + 공정 재측정 완료(2026-06-19)**: 컨슈머 병목 개선 = ①토픽 파티션 1→4 ②concurrency=8+max-poll=2000 ③`LandingWriter` 테이블별락.
  - ★ **공정 재측정**(`scripts/consumer-bench.sh`, 동일 max-poll+오프셋 earliest 리셋+랜딩 plateau 종료판정): 단일 5.88s / Range8 3.65s / **CoopSticky8 3.07s**. 셋 다 최종 580,069줄 동일.
  - ★ **옛 "70배" 폐기** → 공정 병렬효과는 **~1.9배**(5.88→3.07). 96s→3.07s=31배는 대부분 설정개선(옛 max-poll500+전역락) 덕, 스레드 덕 아님.
  - ★ **발견①** 전략이 일꾼 절반 놀림: 기본 RangeAssignor면 8명 중 **4명만 일함**(각 7p), **CooperativeSticky로 바꿔야 8명 전원**(각 3~4p). `--describe`로 실증.
  - ★ **발견②** 8/8 전원가동해도 **1.9배 천장** = 병목이 컨슈머→쓰기로 이동. order_item이 52%(30만)인데 **테이블 단일락 직렬**(쓰기시점=`synchronized(h){writer.write}`). 직렬하한 3.04s ≈ 실측 3.07s. → 다음개선=컨슈머증설 아닌 **order_item 쓰기 샤딩**.
  - 상세 `docs/BENCHMARK.md`(전체여정 S0→S3), 개념 `docs/KAFKA-CONSUMER.md`(전략·리밸런스).
  - ⚠️ 토픽 4파티션, application.yml concurrency=8 영구 반영. **전략은 application.yml 미명시=기본 RangeAssignor**(측정은 런타임 주입으로 비교).
- **관측 스택(Grafana 실시간 모니터링) ✅ 추가(2026-06-18)**: compose에 `kafka-exporter`(9308)·`prometheus`(9090)·`grafana`(3000, 익명접속). 설정 `infra/observability/`.
  - 대시보드 2개(자동 프로비전): **Kafka 실시간**(`/d/settlement-kafka` — LAG총합·토픽별LAG·소비율·유입율) / **자바 컨슈머 앱**(`/d/settlement-app` — 랜딩적재율·JVM힙·스레드·GC).
  - 자바앱 메트릭: `normalize`에 actuator+micrometer-registry-prometheus 추가, server.port=8090, `/actuator/prometheus` 노출. 커스텀 카운터 `settlement.landed.records`(CdcSinkListener). Prometheus가 `host.docker.internal:8090` 스크랩.
  - ★ 대시보드 통합(2026-06-19): kafka+app 2개 → **단일 대시보드**(uid=`settlement`, row로 구획). 패널 추가: 스레드별 담당파티션수(`assigned_partitions{client_id}`)·스레드별 처리량.
  - ★ **스레드↔토픽-파티션 배정 가시화**(2026-06-19): 커스텀 메트릭 `settlement_consumer_assignment{client_id,topic,partition}`(자바 `AssignmentMetrics`=ConsumerAwareRebalanceListener, `KafkaConfig`로 컨테이너팩토리 배선). Grafana Table 패널로 "어느 스레드가 어느 토픽-파티션" 실시간+리밸런스 변동 표시. (카프카 기본 메트릭은 client_id별 개수만 → 토픽매핑은 앱이 직접 라벨링해야 함을 실증)
  - ★ 전략 영구반영: `application.yml`에 `partition.assignment.strategy=CooperativeStickyAssignor` 추가 → 켜면 8스레드 전원 가동(Range는 4명만).
  - ★ 토픽 현재 **4파티션**, application.yml `listener.concurrency=8`+`max-poll-records=2000`, LandingWriter 테이블별락. (병렬화 반영됨)
  - ⚠️ 주의: 오프셋 리셋(`--reset-offsets`)은 **컨슈머 정지 상태**에서만 됨(활성 시 거부). 그래서 앱 켠 채 되감기는 무효.
  - ⚠️ 측정 교훈: 컨슈머 처리시간은 LAG/오프셋 기반이 불안정(조기종료·exporter 갱신지연) → **랜딩 파일 줄 수 plateau(flush 포함)**로 측정해야 정직. → **단일 vs 병렬 공정 비교 완료(2026-06-19)**: 위 4-A(병렬화) 공정 재측정 참조. ~1.9배(70배 폐기).
- **Phase 5-1 order_item 쓰기 샤딩 ✅ 완료(2026-06-19, feat/phase5-sharding)**: `LandingWriter`가 `<table>.p<partition>.ndjson`로 파티션별 분리(한 파티션=한 스레드=전용파일=경합0), 읽기측 전부 `<table>.p*.ndjson` 글롭(e2e_load·inc_run·dbt staging·build-iceberg·landing-to-minio).
  - ★ **가설 반증(정직)**: "order_item 단일락=병목" 가설로 샤딩했으나, 락 제거해도 천장 안 내려감(CoopSticky8 3.07→2.96s, 노이즈). → 단일락 진짜 병목 아님. **진짜 병목은 미확인(측정 안 함 → 단정 금지)**. 상세 `BENCHMARK.md` Phase5.
  - ★ 정합성 유지: E2E 5종 체크섬 정답지 비트동일(16샤드 글롭+`_seq` dedup → 무손실). 교훈: 가설은 측정으로 검증.
  - 다음: 진짜 병목 국소화 측정(①데이터 10배 ②읽기만vs적재 ③스레드별 처리량) — 측정 전엔 병목 단정 안 함.
- **App-2 컨슈머 견고화(DLQ·재시도) ✅ 완료(2026-06-22, feat/app2-consumer-hardening)**: 실패 메시지 유실 방지.
  - `CdcParseException`(non-retryable)=악성 메시지 바로 DLQ / `KafkaConfig` 에러핸들러(FixedBackOff 3회 + `DeadLetterPublishingRecoverer`) / producer(KafkaTemplate) / `settlement.dlq.records` 카운터.
  - DLT 토픽 = `<topic>.DLT`. ★ 검증으로 버그2개 수정: ①발행 파티션 불일치→유실 → 파티션 -1(프로듀서 결정) ②패턴이 .DLT 재구독→cascade → 패턴 `[^.]+$`로 .DLT 제외.
  - 검증: 악성 주입→DLT 격리·카운터·컨슈머 생존·정상28파티션 유지·cascade없음 전부 확인. ⚠️ retryable(IO)→재시도→DLQ 경로는 설정만, 라이브 미검증(주입 어려움).
  - 학습 후보(엔드피처): App-2(완료) → Phase6 Iceberg/Nessie → 7 Spark → 8 Avro/SchemaReg → 9 Airflow → 10 서빙API. 상세 설계 대화 참조.
- **App-3 dbt incremental ✅ 완료(2026-06-30, feat/app3-dbt-incremental)**: 손작성 watermark 증분(3-4)을 dbt 네이티브 incremental로.
  - staging 4개(stg_payment·payment_method·order_item·cancel) → `materialized=incremental`, `_seq`(LSN) watermark(`_seq > (SELECT MAX(_seq) FROM {{this}})`), `delete+insert`(unique_key), `post_hook="DELETE WHERE _op='d'"`(dbt 네이티브 하드삭제 미지원 보완). `_op`·`_seq` 컬럼 유지. int_*·마트는 table 유지.
  - `run-dbt.sh`에 `inc` 모드 추가(db 미삭제 → {{this}}=watermark). build/run=전체(초기화), inc=증분.
  - ★ 검증: V1 첫빌드=정답지 값 비트동일(5테이블,캐스팅정규화)+30테스트 / V2 무변화 재실행=멱등 / V3 1건 UPDATE 주입→watermark로 그 행만 증분반영(건수불변·watermark전진)·주입 정리 복원.
  - ⚠️ 체크섬 주의: dbt `CREATE TABLE AS` 타입추론(HUGEINT/JSON) vs 손작성 DDL(BIGINT) 차이로 raw hash는 불일치 → **값 비교는 캐스팅 정규화 필요**(데이터는 동일). App-3 회귀 아님.
- **Phase 6/7 Spark 레이크하우스 ✅ 완료(2026-07-01~02, feat/phase6-spark-lakehouse)**: PyIceberg 건너뛰고 바로 Spark(버릴 코드 방지 — 사용자 결정). Nessie 카탈로그 위에서 Spark로 Iceberg 메달리온.
  - **6-1 Nessie ✅**: `sp-nessie`(0.99.0, :19120) Iceberg REST 카탈로그, warehouse=s3://lake/warehouse. ★설정은 `infra/nessie/application.properties`(Quarkus env는 ./- 모호성으로 S3 설정 미매핑 — 검증으로 발견), 자격증명=시크릿 URN 패턴(`urn:nessie-secret:quarkus:minio-creds`), healthcheck=/iceberg/v1/config(기본 /q/health는 404).
  - **7-1 Spark 연결 ✅**: `scripts/spark-sql.sh` — burst(docker run --rm, DuckDB/dbt 패턴), apache/spark:3.5.3 + iceberg-spark-runtime/aws-bundle 1.7.1, ivy 캐시 볼륨. 스모크(`transform/spark_smoke.sql`) 통과. 잡은 문제 3: ①ivy 권한(--user root) ②Nessie env 미매핑→properties ③auth-mode/access-key 키 0.99 스키마 불일치→URN 패턴.
  - **7-2 메달리온 ✅**: `transform/spark_bronze.sql`(랜딩 샤드→bronze 4테이블, 건수=랜딩 580,069 일치) / `spark_silver.sql`(dedup canonical 4+마스터 3, 고아 0,0) / `spark_gold.sql`(03정산 포팅 — ASOF JOIN→row_number 최신1건, TEMP테이블→TEMP VIEW, ::DOUBLE→CAST, ||→concat).
  - ★ **정합성**: gold settlement_agg 9,795행/event 511,828행 = DuckDB 정답지와 **건수·값체크섬(캐스팅 정규화) 완전 일치**, recon 0,0. 검증은 Spark export(data/spark_export)를 DuckDB로 읽어 같은 엔진에서 대조.
  - ★ **append/타임트래블 실증**: appenddemo 2배치(4,936+4,984=9,920) append → 옛 커밋 시점 조회=4,936 성공. **발견: Nessie는 이력을 Iceberg 스냅샷 리스트가 아니라 Nessie 커밋(데이터의 Git)으로 관리**(스냅샷 목록엔 현재 1개만) → 타임트래블은 커밋 해시로 카탈로그 uri 고정(`/iceberg/main@<hash>/`).
  - HTML 2종: `docs/architecture.html` 전면 재작성(인터랙티브 — 노드 클릭 상세·상태 배지·로드맵, v1→현행), `docs/data-flow.html` 신규(주문 64823이 10단계로 변해가는 실데이터 예시, 스텝 네비).
  - ★ **풀 E2E ✅ 완료(2026-07-02)**: 전체 초기화(랜딩·토픽 delete-records·DLT 삭제·Nessie 재시작·warehouse 비움) → raw 프로듀스(PG 적재)→CDC 580,069→컨슈머(16샤드 580,069)→Spark bronze(건수 일치)→silver(고아 0,0)→gold(9,795/511,828, recon 0,0)→**정답지와 건수·값체크섬 비트 동일**. 빈 상태에서 전 구간 1회 관통 증명.
  - 관측 갱신(2026-07-02): Grafana에 DLQ 격리 패널 추가(앱 카운터+브로커 DLT 적재), 유입율 정규식 `.DLT` 제외, 스레드 배정표 합친컬럼(토픽-파티션) 라이브 반영.
- 4-B 분산(Spark) ✅ ← Phase 6/7로 완료 (위 참조 — Nessie/Iceberg 위 Spark, burst 모드. 진짜 멀티노드 클러스터는 추후 학습 후보)
- **Phase 8 Avro + Schema Registry ✅ 완료(2026-09-01, feat/phase8-avro-schema-registry)**: CDC 메시지를 스키마리스 JSON → Avro(Apicurio 레지스트리)로 전환, 스키마 진화 검증.
  - 인프라: `sp-apicurio`(kafkasql 스토리지 — 스키마가 Kafka 토픽에 저장돼 kafka-data 볼륨으로 영속, :8085). 버전 2.4.1.Final = debezium/connect 내장 컨버터와 정확히 일치(실측). Connect에 `ENABLE_APICURIO_CONVERTERS=true` 필요(jar는 내장, 활성화 env).
  - 커넥터: value만 AvroConverter(auto-register), key는 JSON 유지(컨슈머 키 경로 무변경). ★ 같은 이름 재등록 시 `_connect_offsets`에 옛 오프셋이 남아 스냅샷 생략됨 → **등록→stop→`DELETE /connectors/{name}/offsets`→resume**으로 강제 재스냅샷(Kafka 3.7 API). 재스냅샷 580,069 = 기준선 정확 일치.
  - 컨슈머: `AvroKafkaDeserializer`를 `ErrorHandlingDeserializer`로 래핑(poison→poll 무한루프 대신 DLQ). CdcParser JSON→GenericRecord 전환(Utf8→String 등 표준화, 스키마에 없는 필드 null 방어). DLT 발행은 값 타입별 템플릿 2종(역직렬화 실패=원본 byte[]·리스너 실패=Avro 재직렬화). 테스트 13종 Avro 픽스처로 재작성, 전부 통과.
  - ★ DLQ poison 라이브 검증: 쓰레기 바이트 주입 → 재시도 루프 없이 `<topic>.DLT`에 원본 byte[] 그대로 격리, 카운터+1, 컨슈머 생존. 이전 세션 미커밋 오프셋 재소비(동일 _seq 중복 d)는 읽기 dedup이 자연 흡수 — at-least-once 설계 실증. (리스너 실패→Avro 재직렬화 DLT 경로는 설정만 검증 — App-2 때와 동일 수준)
  - ★ 정합성: Avro 랜딩 580,069줄 → 재구성 settlement_agg 9,795행 **체크섬 = DuckDB 정답지와 비트 동일** (JSON→Avro 전환 무손실 증명).
  - ★ 진화 실증①(호환): cancel `ADD COLUMN memo` → 새 이벤트에서 스키마 v2 자동 등록, **코드 무수정 컨슈머가 무중단 처리**(landed+1, DLQ 0), projection이 memo 자연 제외. 삭제 전파(c→d, LSN 단조)도 확인.
  - ★ 진화 실증②(거부): BACKWARD 규칙 하에 필드 추가(옵셔널)=200 통과 / `reason_code` int→string 타입 변경=**409 RuleViolationException 거부**.
  - ⚠️ 한계(실측): Apicurio 2.4.1은 **참조(Source) 포함 아티팩트에 호환성 규칙을 걸면 기존 버전 파싱 실패(422)** → 실제 CDC 아티팩트엔 규칙 미적용 상태(진화는 auto-register로 무규칙 등록). 규칙 실사용은 레지스트리 업그레이드 또는 Source 인라인 전환 필요.
  - ⚠️ 랜딩 백업: `data/landing.pre-avro/`(JSON 시절) 보관 중 — 검증 끝났으니 삭제 가능.
- **Phase 9 Airflow 오케스트레이션 ✅ 완료(2026-09-01, feat/phase9-airflow)**: 수동 셸 순서 실행 → DAG.
  - 인프라: `sp-airflow`(apache/airflow 2.10.5 standalone·LocalExecutor, :8087), 메타DB=sp-postgres `airflowdb`, 이미지에 docker CLI 정적 복사(docker:27.5.1-cli 멀티스테이지).
  - ★ 핵심 트릭: 저장소를 **호스트와 동일 절대경로**(`REPO_HOST_PATH`, infra/.env)로 마운트 + docker.sock 공유 → 기존 `scripts/*.sh`의 `docker run -v "$PWD":/work` 패턴을 **무수정 재사용**. (root·sock 공유는 로컬 타협 — PRODUCTION-CHECKLIST §5)
  - DAG `settlement_close`: landing_to_minio → build_iceberg → settle_and_check (재시도 1회, @daily 정의·생성 시 paused).
  - ★ 함정 실측: bash_command가 `.sh`로 끝나면 Jinja 템플릿 파일로 오인(TemplateNotFound) → 끝 공백으로 회피(DAG 주석 기록).
  - 검증: 1차 실패 런에서 재시도 소진→failed·하류 upstream_failed 전파(실패 시나리오 겸증) / 2차 런 3태스크 success(21초), 산출 settlement_agg 9,795행 **체크섬 정답지와 비트 동일**.
- **Phase 10 서빙 API ✅ 완료(2026-09-01, feat/phase10-serving-api)**: 정산 조회 Spring Boot API — 로드맵 학습 후보 완주.
  - 구조: 레이크 산출(e2e_iceberg.duckdb) → `publish-serving.sh`(TRUNCATE+COPY 멱등, 건수 자체검증) → **sp-postgres `servedb`**(DDL=infra/postgres/serving.sql) → `serving/` JPA read-only API(:8091 루프백). DAG 4번째 태스크 `publish_serving`으로 오케스트레이션 편입.
  - 서빙 스토어를 분리한 이유: DuckDB 단일 writer(배치 재작성과 조회 충돌) + 레이크→서빙 적재가 실무 표준 + 확정 설계(서빙=JPA·ID 참조 패턴).
  - API: `GET /api/v1/settlements/{date}`(파트너 요약, 페이징) · `/{date}/partners/{id}`(수단별 상세, 없으면 404). 전 엔드포인트 X-API-Key(env `SERVING_API_KEY`, 상수시간 비교), 입력 검증(ISO 날짜·@Positive), 오류 응답에 내부 정보 미노출.
  - 검증: 키 없음/오류=401 · 파트너1 CARD 157,817,622/3,945,482/153,872,140 = **04_checks 출력과 정확히 일치** · 테스트 8/0/0(given/when/then, 합성 픽스처) · gradle.lockfile 적용.
- **운영 전환 문서 ✅ (2026-09-01)**: `docs/PRODUCTION-CHECKLIST.md` — 로컬 타협 전수(통신·인증·시크릿·가용성·공급망·스키마 거버넌스·관측·도메인) → 운영 요구 + 착수 우선순위.

## 남은 일 (우선순위)

1. ~~본 DB에 v3 반영 + 멱등 재확인~~ ✅ 완료 + 재현성까지 해결(2026-06-11)
2. 텍스트 문서 v3 동기화 ✅ 완료(2026-06-11): SCHEMA·DATA-LAYOUT·DOMAIN-NOTES·ROADMAP·DESIGN·ENVIRONMENT
   - architecture.html ✅ 재작성(2026-07-02) · ⬜ 남음: **다이어그램 HTML 2종**(system-architecture/aws-architecture) — v1 기준, 재작성 필요
3. (옵션) 결제수단별 차등수수료를 partner×method 2차원으로 확장 / 환불 대사(정산 밖이라 보류)
4. ~~2단계: 실시간 수집~~ ✅ 완료(2026-06-17) — 이후 진행 상황은 위 "2단계~" 섹션들 참조
