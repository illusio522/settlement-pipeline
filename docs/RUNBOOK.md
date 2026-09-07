# RUNBOOK — 구동·확인·복구 절차

> 아무것도 안 떠 있는 상태(콜드스타트)부터 풀가동까지, 그리고 자주 겪는 복구 시나리오.
> 전제: `infra/.env` 존재 (`infra/.env.example` 복사 후 값 채움).

---

## 1. 콜드스타트 (전체 타임라인)

```
⓪ Docker 데몬        open -a Docker  →  docker info 응답까지 대기
①② 컨테이너 스택     cd infra && docker compose up -d          (11개, 순서는 자동)
③ 커넥터 등록        bash scripts/register-connectors.sh       (보통 "이미 등록됨" no-op)
④ 컨슈머            cd normalize && ./gradlew bootRun          (수동 · 밀린 분 따라잡음)
⑤ 서빙 API (필요시)  cd serving && ./gradlew bootRun            (:8091)
⑥ 정산 배치 (필요시)  Airflow(:8087)에서 settlement_close 트리거
```

★ **②까지 올라가면 데이터는 이미 안전** — 캡처(Debezium)·보관(slot·브로커)이 전부 컨테이너 안.
④~⑥은 "밀린 일을 언제 처리할까"의 스위치일 뿐, 늦어도 유실 없음.

### 컨테이너들 사이·안의 순서 (compose가 자동 편성)

```
1파도(병렬): postgres · kafka · minio
   postgres 내부: 볼륨 비었을 때만 init.sql (DB·테이블·publication 생성) → healthy
   kafka 내부:    KRaft 컨트롤러 합의 → 리스너 오픈 → kafka-data의 토픽·오프셋 로드
2파도(healthy 대기 후):
   connect(kafka+postgres) — 내부: 런타임 → _connect_configs에서 커넥터 복원
                              → slot 재접속 → _connect_offsets의 LSN부터 스트리밍 재개
                              ← 재등록 없이 CDC가 살아나는 지점
   apicurio(kafka — kafkasql서 스키마 로드) · kafka-ui · kafka-exporter
   nessie(minio) · airflow(postgres — 마이그레이션 → DAG 스캔, paused 유지)
3파도: prometheus(exporter) → grafana(prometheus)
```

원리: `depends_on: service_healthy` — "떴다"가 아니라 "일할 수 있다"를 기다림.
한 번 정상 기동된 뒤에는 순서에 둔감 (각자 재시도 + slot·브로커·오프셋이 보관).

---

## 2. 정상 확인 체크리스트

```bash
docker ps --format '{{.Names}} {{.Status}}'          # 11개, healthcheck 있는 것 전부 healthy
curl -s localhost:8083/connectors                     # ["order-connector","pay-connector"]
curl -s localhost:8083/connectors/pay-connector/status | grep -o '"state":"[A-Z]*"'   # RUNNING×2
```

데이터 기준선 (합성 시드 기준):
```bash
# orderdb: order_item 300,000 · cancel 9,920 / paydb: payment 104,980
docker exec sp-postgres psql -U "$POSTGRES_USER" -d orderdb -tAc "SELECT count(*) FROM order_item"
# 토픽 7개 × 4파티션, 총 580,069 메시지
docker exec sp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe \
  | grep -c 'PartitionCount: 4'
```

UI: Airflow :8087 · Grafana :3000/d/settlement · Kafka UI :8081 · Apicurio :8085 · MinIO :9001
(전부 127.0.0.1 바인딩 · 호스트 8080은 이 프로젝트와 무관한 별도 nginx가 점유 — 건드리지 말 것)

---

## 3. 복구 시나리오

### A. Docker 데몬이 죽어 있음 (가장 흔함)
```bash
open -a Docker && until docker info >/dev/null 2>&1; do sleep 3; done
cd infra && docker compose up -d        # 컨테이너는 restart 정책 없음 → 수동 up 필요
```
데이터 유실 없음 (named 볼륨: pg-data·kafka-data·minio-data·airflow-data).
컨테이너 재생성 중 포트 바인딩 경합으로 `address already in use`가 한 번 날 수 있음 → `up -d` 재실행.

### B. kafka-data 볼륨까지 소실
토픽·`_connect_configs`·오프셋·스키마(kafkasql) 전멸. 원천(Postgres)은 무사.
```bash
docker compose up -d
bash scripts/register-connectors.sh     # 이번엔 진짜 등록 → 오프셋 없음 → 자동 재스냅샷
```
스냅샷 완료 기준: 토픽 총 메시지 = 기준선(580,069). 랜딩은 append+읽기 dedup이라 재처리 무해.

### C. 강제 재스냅샷 (볼륨은 살아있는데 처음부터 다시 흘리고 싶을 때)
같은 이름 재등록은 옛 오프셋 때문에 스냅샷이 생략된다 (P8 실측). 절차:
```bash
for c in pay-connector order-connector; do
  curl -X PUT  localhost:8083/connectors/$c/stop
  curl -X DELETE localhost:8083/connectors/$c/offsets
  curl -X PUT  localhost:8083/connectors/$c/resume
done
```

### D. slot 밀림 (WAL 적체 — 원장 DB 디스크 위험)
```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS 밀린양
FROM pg_replication_slots;
```
active=f + 밀린양 증가 = 죽은 구독자가 WAL을 잡고 있음 → 컨슈머(Connect) 복구가 우선,
영구 폐기라면 `SELECT pg_drop_replication_slot('<slot>')`.

### E. 컨슈머 정지 (호스트 앱)
```bash
kill -TERM $(lsof -ti TCP:8090 -sTCP:LISTEN)    # ★ 반드시 -sTCP:LISTEN — 리스너만!
```
`lsof -ti :8090`(조건 없이)은 그 포트에 *연결 중인* com.docker.backend까지 잡아
**Docker 전체를 죽인다** (2026-09-01 실사고 2회). graceful 종료여야 close() flush로 버퍼 유실 없음.

---

## 4. 자주 쓰는 명령

```bash
# 토픽 메시지 수 합계
docker exec sp-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
  --topic-partitions 'pay\.public\..*,order\.public\..*' | awk -F: '{s+=$3} END {print s}'
# 컨슈머 그룹 LAG
docker exec sp-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group normalize-sink --describe
# 메시지 열람 (key 포함)
docker exec sp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pay.public.payment --partition 0 --offset 0 --max-messages 1 --property print.key=true
# DAG 수동 트리거 (UI 대신 CLI)
docker exec sp-airflow airflow dags trigger settlement_close
# 정산 결과 조회 (서빙 API — X-API-Key는 infra/.env)
curl -H "X-API-Key: $SERVING_API_KEY" localhost:8091/api/v1/settlements/2026-05-15
```
