# 검증 방법 (테스트 케이스·근거)

> 각 단계에서 "무엇이 맞아야 하는가"와 "어떻게 증명했는가"를 기록.
> 원칙: 정답지 맹신이 아니라 **서로 다른 축의 독립 검사 동시 통과**로 신뢰를 삼각측량.

---

## 검증 철학

| 구분 | 묻는 것 | 보장 방법 |
|---|---|---|
| **A. 로직 정확성** | 정산 계산 자체가 옳은가 | 독립 불변식(돈 보존·MID 대사·결정성·손검산) 동시 통과 |
| **B. 운반 충실성** | 파이프라인이 데이터를 보존하는가 | E2E 차분 검증(1단계 결과 = 2단계 결과 체크섬) |

- B는 **상대 검증** — 정답지가 "1단계 결과"인 게 맞는 기준. 로직 버그가 있어도 양쪽 동일하면 운반 충실성은 유효 증명.
- 어떤 검증도 절대 정확성을 증명하진 못함 → 틀리면 걸릴 함정을 여러 개 놓고 전부 통과 = 신뢰 축적.

핵심 도구: **체크섬**(`BIT_XOR(hash(...))`) — 순서 무관 집계라 행 순서 달라도 내용 같으면 동일값. 두 데이터셋의 동일성을 한 숫자로 비교.

---

## 1단계: 정산 배치 로직 (DuckDB)

검증 스크립트: `transform/04_checks.sql` / 실행: `bash scripts/run-step1.sh`

| # | 테스트 | 통과 기준 | 방법 |
|---|---|---|---|
| 1 | **돈 보존** | `recon_result` diff=0 | 결제원천 PAY합 = 집계 gross합 (2단 잔액보정으로 반올림 누수 0) |
| 2 | **MID 대사** | `recon_card` diff=0 | 주결제수단(카드·머니) MID별 합 = PG 정산파일 |
| 3 | **차등수수료 freeze** | 카드 5월2.5%/6월2.3%, 머니1.5%, 포인트·쿠폰0% | as-of `method_fee` ASOF JOIN, 거래시점 박제 |
| 4 | **복합결제** | Σ수단 = 헤더, method별 백데이터 | 라인×결제수단 교차배분 |
| 5 | **부분취소** | 취소 라인만 셀러 정산 차감 | `cancel` 라인명세 조인 |
| 6 | **멱등성** | 원장 고정·정산 재실행 → agg/event 체크섬 비트 동일 | TRUNCATE+재생성 결정성 |
| 7 | **재현성** | 시드 재생성 2회 → 6개 소스/파생 체크섬 전부 동일 | random() 비결정 차단(hash 배정+단일스레드). 상세 DATA-LAYOUT §6 |

> 멱등성 ≠ 재현성: 멱등=원장고정·정산만 재실행 동일 / 재현=시드재생성 동일. `run-step1.sh`는 원장까지 새로 만들어 멱등 검증용 아님.

---

## 2단계: 실시간 수집 (Postgres→CDC→Kafka→컨슈머→랜딩)

각 하위 단계마다 **그 구간이 데이터를 흘리는가**를 라이브로 검증.

### 2-1 Postgres
| 테스트 | 기준 | 방법 |
|---|---|---|
| wal_level | = `logical` | `SHOW wal_level` |
| 도메인 분리 | paydb 5테이블 / orderdb 2테이블 | `\dt` |
| 물리 FK 없음 | FK 제약 0개, 고아 INSERT 허용됨 | `pg_constraint` 조회 + 고아 행 삽입 성공 |
| publication | `dbz_pay`(5)·`dbz_order`(2) | `pg_publication_tables` |

### 2-2 Kafka
| 테스트 | 기준 | 방법 |
|---|---|---|
| KRaft 쿼럼 | LeaderId·Voters 정상 | `kafka-metadata-quorum describe --status` |
| produce/consume 왕복 | 3건 써서 3건 그대로 | console-producer → console-consumer |
| 호스트 리스너 | 29092 열림(자바앱용) | `nc -z localhost 29092` |

### 2-3 Debezium (CDC)
| 테스트 | 기준 | 방법 |
|---|---|---|
| 커넥터 상태 | pay·order 둘 다 RUNNING | REST `/connectors/<n>/status` |
| replication slot | active=t | `pg_replication_slots` |
| CDC INSERT 왕복 | Postgres INSERT → 토픽에 `op=c`+after 도착 | 1건 넣고 console-consumer로 확인 |
| CDC DELETE | `op=d`(before) + 톰스톤(null) | 삭제 후 토픽 메시지열 확인 |
| 토픽 자동생성 | 데이터 흐른 테이블만 `<prefix>.public.<table>` | 토픽 목록 |

### 2-4 싱크 (Spring Boot 컨슈머)
| 테스트 | 기준 | 방법 |
|---|---|---|
| 카프카 구독 | partitions assigned 로그 | 앱 기동 로그 |
| 파싱 | 7테이블 op분기(c=after/d=before)·톰스톤 스킵 | 복합결제 1건 넣고 파싱 로그 |
| **정규화** | 4원장→method 1·2·3·4, 보조 mid=null, Σ수단=헤더 | canonical 로그 확인 |
| projection | payment서 currency·status·audit 제거 | canonical 컬럼 확인 |
| **멱등(B방식)** | 재처리로 24줄 쌓여도 dedup 4행(중복제거+삭제반영) | 오프셋 reset 후 재처리 → DuckDB dedup 쿼리 |

---

## 2-5 E2E 검증 (2단계 피날레)

**목표**: 데이터가 전 구간 통과 후에도 1단계와 동일함 증명 (운반 충실성).

### 방법 (차분 검증)
```
1단계 canonical (정답지)
   → CSV export (payment_method를 4원장 분리)
   → Postgres 현실형 적재 (현실 컬럼은 DEFAULT)
   → Debezium CDC → Kafka (58만 건)
   → 컨슈머 → 랜딩 NDJSON
   → 읽기 dedup (key별 _seq 최대 & _op<>'d') → canonical 재구성
   → 03 정산배치
   → settlement_agg
   == 1단계 settlement_agg 체크섬과 동일?
```
스크립트: `scripts/e2e-load-postgres.sh`(적재) → `scripts/run-e2e.sh`(재구성)

### 단일 출처 원칙
두 곳에서 따로 생성하면 hash 함수가 달라 데이터 어긋남 → **1단계를 정답지로 export**해 Postgres에 적재. (Postgres hashtext ≠ DuckDB hash)

### 깨끗한 환경
시험 데이터 오염 방지 → 토픽·컨슈머그룹 purge 후 시드만 적재 (옛 데이터는 dedup이 삭제로 걸러내지만 명확성 위해 purge).

### 통과 기준 (2026-06-17 전부 통과)
| 검사 | 결과 |
|---|---|
| 적재 건수 | payment 104980·수단합 165169·order_item 300000·cancel 9920 = 정답지 일치 |
| CDC 전파 | 7토픽 메시지 수 = 적재 건수 |
| 랜딩 건수 | 4파일 = 정답지 일치 |
| 고아 검증 | pm_no_header=0, cancel_no_line=0 |
| 돈 보존/MID 대사 | recon_result·recon_card diff=0 |
| **체크섬 일치** | payment·payment_method·order_item·cancel·**settlement_agg** 5종 전부 비트 동일 |

→ 파이프라인이 데이터를 **무손실·무변형 운반**(4원장 분리·재통합, epoch 변환, dedup 거쳐도 동일) 증명.

---

## 검증 도구 모음

| 도구 | 용도 |
|---|---|
| `BIT_XOR(hash(...))` | 순서무관 체크섬 — 데이터셋 동일성 비교 |
| `recon_result`/`recon_card` | 돈 보존·외부 대사 (독립 축) |
| 고아 검증 쿼리 | 물리 FK 제거 보완 (LEFT JOIN ... IS NULL) |
| `kafka-consumer-groups --describe` | LAG (소비 진행) |
| `wc -l data/landing/*.ndjson` | 랜딩 적재 건수 |
| dedup 쿼리(row_number) | 멱등 검증 (중복·삭제 정리) |
