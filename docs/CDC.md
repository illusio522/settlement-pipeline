# CDC — 캡처·발행 측 해설 (Postgres → Debezium → Kafka)

> KAFKA-CONSUMER.md(소비 측)의 짝 문서. 커넥터 설정 한 줄 한 줄이 무엇을 하는지,
> 메시지가 어떻게 태어나는지, slot·WAL을 어떻게 운영하는지.
> 근거: 이 저장소의 실설정·실측 (2026-09 문답 시리즈 정리).

---

## 1. 원리 — CDC는 "DB가 어차피 쓰는 일지"의 구독

모든 DBMS는 장애 복구를 위해 **커밋 전에 변경을 순차 로그(WAL)에 먼저** 적는다
(테이블 파일 반영은 나중에 천천히 — 정전 시 WAL 재생으로 복구).
CDC는 이 로그를 복제 프로토콜로 구독한다 → 폴링도 트리거도 아니라
**소스 트랜잭션을 느리게 하지 않고, 변경을 빠짐없이 커밋 순서대로** 받는다.

```
INSERT/UPDATE/DELETE → ①WAL 기록(LSN 부여) → ②테이블 반영
Debezium은 ①의 스트림을 수신 (테이블은 안 건드림 · WAL 파일 직접 접근도 없음 — 서버가 읽어줌)
```

- `wal_level=logical` : WAL에 "행의 논리적 내용"까지 기록하게 격상 (CDC의 유일한 소스측 전제.
  기본 replica 레벨은 복구·물리복제용이라 행 해석 불가)
- **LSN** : WAL 테이프 위의 바이트 위치. DB 전역 단조 증가 → 랜딩 `_seq`의 원천
  (여러 토픽을 합류시켜도 최신 판별이 흔들리지 않는 유일한 좌표 — STATUS 3-4의 교훈)

WAL은 CDC와 무관하게 항상 생성된다. 위치: 데이터 디렉토리 안 `pg_wal/` (16MB 세그먼트 파일들).

---

## 2. 커넥터 설정 항목별 해설 (`infra/debezium/*.json`)

커넥터 2개 = 도메인 DB 2개(paydb/orderdb). 골격 동일, 대상만 다름.

### 그룹① 접속
| 항목 | 값 | 설명 |
|---|---|---|
| `connector.class` | PostgresConnector | DB 교체 시 바뀌는 지점 |
| `database.hostname` | `postgres` | 도커 서비스명 DNS (Connect도 같은 네트워크의 내부 손님) |
| `database.user/password` | `__PLACEHOLDER__` | 실값은 등록 스크립트가 infra/.env로 치환 (§5) |

### 그룹② 캡처 — 셋은 서버에 실존, 둘은 커넥터 방침
```
Postgres 서버 안에 실존:  pgoutput(번역기) · publication(발행물) · slot(구독자 명부)
커넥터의 행동 방침:       snapshot.mode · table.include.list
```
| 항목 | 설명 |
|---|---|
| `plugin.name: pgoutput` | WAL 바이너리→논리 변경 번역기. PG10+ **내장**이라 설치 불요 (wal2json 등 대안은 설치 필요) |
| `publication.name` | **서버측 필터** — init.sql에서 `CREATE PUBLICATION ... FOR TABLE 5개`로 우리가 생성. 미포함 테이블 변경은 전송 자체가 안 됨 |
| `slot.name` | 서버측 책갈피+보관계약 (§3). **이름만 여기서 선언, 실물은 Debezium이 최초 기동 때 생성** — init.sql에 없는 이유. 커넥터당 1개(DB 단위) |
| `snapshot.mode: initial` | **진도 기록(_connect_offsets)이 없을 때만** 전체 스냅샷(SELECT *, op='r') 후 스트리밍 전환. WAL엔 과거가 없으므로 신규 구독자는 복사부터 |
| `table.include.list` | **커넥터측 2차 필터** + 스냅샷 대상 목록. publication(서버가 내보내는 범위)과 역할 분리 — 보통 일치시키지만 커넥터별로 좁힐 수 있음 |

### 그룹③ 직렬화 — 메시지의 두 칸에 다른 옷
| 항목 | 결과 |
|---|---|
| `key.converter: JsonConverter` + `schemas.enable: false` | key = PK만 담은 JSON 텍스트 `{"order_no":64823,"line_no":1}` (스키마 동봉 끔 — 알맹이만) |
| `value.converter: AvroConverter` (Apicurio) | value = `[0x00][스키마 글로벌ID][Avro 바이너리]`. 스키마는 레지스트리에 계약으로 |
| `...auto-register: true` | 새/변경 스키마를 발행 시점에 자동 등록 (운영은 끄고 CI 등록 권장 — PRODUCTION-CHECKLIST §6) |

★ 문서·UI에서 보는 `{"before":...,"after":...}` JSON은 **표기(사람용 그림)**이고,
value의 실물은 Avro 바이너리다. "JSON을 거쳐 Avro가 되는" 단계는 없다 —
WAL → Struct(자바 객체) → Avro 직행. key만 진짜 JSON.

### 그룹④ 타입 매핑
| 항목 | 함의 |
|---|---|
| `decimal.handling.mode: double` | NUMERIC→실수. **돈에 double은 원래 금기**지만 우리 금액은 전부 BIGINT(원 단위)라 영향권 밖. NUMERIC 금액이 생기면 재고할 것 |
| `time.precision.mode: connect` | 타임스탬프→epoch 밀리초(long). 랜딩 event_time이 숫자인 이유 (DuckDB에서 `epoch_ms()` 복원) |

### 그룹⑤ 이름 규칙
```
토픽명 = <topic.prefix>.<스키마>.<테이블>     예: pay.public.payment_card
레지스트리 아티팩트 = <토픽명>-value
```
prefix = 네임스페이스(두 DB 모두 스키마가 public이라 충돌 방지) + 컨슈머 구독 정규식의 손잡이.
토픽은 첫 메시지 도착 시 **자동 생성** — 브로커 `KAFKA_NUM_PARTITIONS: 4` 필수
(기본 1이면 병렬화 무력화 — 2026-08 실사고).

---

## 3. slot — 책갈피 + 보관 계약

역할: **"구독자가 자리를 비워도 서버가 그의 몫(WAL)을 보관하게 만드는 약속."**

```
평상시: 서버 전송 → 구독자 ack → confirmed_flush_lsn 전진 → 그 이전 WAL 재활용 가능
다운시: active=f 되어도 slot이 증언 → 서버가 미확인 WAL을 한 조각도 못 버림 = 유실 방지
부활시: confirmed 지점부터 이어서 전송
```

- ack 순서가 "발행 후 ack"라 **at-least-once** — 중복 가능, 유실 없음.
  중복은 랜딩 읽기 dedup(`_seq`)이 흡수 (설계의 수미상관)
- slot은 커넥터보다 오래 산다 — 커넥터 DELETE 후에도 잔존.
  완전 제거: `SELECT pg_drop_replication_slot('dbz_pay_slot')`
- 동시 접속 1개(active 독점)

**감시 쿼리** (운영 필수):
```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS 밀린양
FROM pg_replication_slots;
```

### WAL 보관 정책 = "필요하다는 자들의 최댓값"
조각 하나는 ①마지막 체크포인트 이후분 ②slot 미확인분 ③wal_keep_size ④아카이브 대기 —
하나라도 해당하면 보관, 전원 통과 시 재활용.

| 설정 | 우리(기본값) | CDC 운영 권장 |
|---|---|---|
| `wal_level` | logical | logical (필수) |
| `max_slot_wal_keep_size` | **-1 (무제한)** ⚠️ | 유한값 + 밀림 알람 세트 (상한 도달 = slot 파산 = 재스냅샷 트레이드오프) |
| `archive_mode` | off ⚠️ | on (PITR 백업 — CDC와 별개 목적) |
| Debezium heartbeat | 미설정 | `heartbeat.interval.ms` — 한가한 DB에서 ack이 멈춰 WAL 적체되는 함정 대응 |

---

## 4. 메시지 탄생 여정 (한 건의 INSERT가 브로커에 안착하기까지)

```
COMMIT
 → WAL 기록 (LSN 부여)
 → [PG] pgoutput 디코딩 + publication 필터 → 복제 스트림
 → [Debezium] Struct 봉투 조립 {before, after, op, source.lsn}
              + key Struct = PK 복사본 (별도 칸 — 파티셔닝·톰스톤·컴팩션은 key 칸만 보므로)
              + 토픽명 결정 (그룹⑤)
 → [컨버터] key=JSON / value=Avro(레지스트리 ID)
 → [프로듀서] hash(key바이트) % 4 → 파티션 → 브로커 append, offset 부여
 → [진도] _connect_offsets에 LSN 기록 + slot ack
```

- **key = 테이블 PK 자동** (별도 설정 없음. 변경은 `message.key.columns`).
  PK 없는 테이블 = key null = 순서·톰스톤 붕괴 → CDC 전제는 PK
- 같은 PK → 같은 파티션 = 그 행의 변경 순서 보장 단위
- DELETE는 봉투(op='d', before) + **톰스톤**(value=null, key만) 두 건 발행

---

## 5. 운영 절차

**등록** — `scripts/register-connectors.sh` (사람이 수동 실행, compose 관할 밖):
- 파일은 설정의 "원본"일 뿐, 효력은 REST POST로 등록된 순간부터 (`_connect_configs` 토픽에 저장)
- Connect 재시작 시 자동 복원 → 재실행 불필요. 스크립트는 멱등(등록돼 있으면 건너뜀)이라 아무 때나 안전
- 실행이 진짜 필요한 순간: 최초 구축 / kafka-data 볼륨 소실
- 치환이 bash 내장(`${line//...}`)+stdin 파이프인 이유: 비밀번호가 `ps`에 노출되는 순간 제거

**설정 변경**: 파일 수정 → 커넥터 DELETE(또는 PUT /config) → 재등록. 파일만 고치면 반영 안 됨.

**강제 재스냅샷** (같은 이름 재등록 시 옛 오프셋이 남아 스냅샷이 생략되는 함정 — P8 실측):
```
등록 → PUT /connectors/{n}/stop → DELETE /connectors/{n}/offsets → PUT /connectors/{n}/resume
```

**소스 DBMS 교체 시** (요지): 커넥터 설정·DB 준비는 교체 작업이지만,
코드의 급소는 `CdcParser`의 `_seq` 추출 — Oracle은 `source.scn`(대응물 존재),
**MySQL은 전역 단조 좌표가 없어**(binlog file+pos/GTID) 좌표 설계부터 다시.
lsn 부재 시 offset fallback이 무증상 오답(다중 토픽 dedup 붕괴)을 만드니 반드시 재설계+테스트.
봉투·하류(정규화→랜딩→정산)는 무수정.
