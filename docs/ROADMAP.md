# 작업 로드맵 — 파트너 정산 파이프라인

> 원칙: **인프라보다 로직 먼저.** 정산 배치 로직을 DuckDB로 검증한 뒤, 그 검증된 로직 앞뒤로 수집·레이크·오케스트레이션 인프라를 붙인다.
> 각 단계는 **명확한 검증 기준(통과해야 다음으로)**을 가진다.

---

## 단계 개요

| 단계 | 목표 | 인프라 | 상태 |
|---|---|---|---|
| 0 | 설계·스킴·다이어그램·DuckDB 도커 | — | ✅ 완료 |
| **1** | **정산 배치 로직 검증** (조인·배분·freeze·집계·대사) | DuckDB | ✅ **완료·검증통과** |
| 2 | 실시간 수집 | Postgres + Debezium + Kafka(KRaft) | ⬜ |
| 3 | 레이크 + 증분 멱등 | MinIO + Iceberg + dbt | ⬜ |
| 4 | 스큐·성능·스키마진화 검증 | (위 스택) | ⬜ |
| 5(opt) | 오케스트레이션·관측·마감·지급 | Airflow/관측 | ⬜ |

---

## ✅ 단계 0 — 설계 (완료)
- 산출물: `docs/DESIGN.md`, `docs/SCHEMA.md`, 다이어그램 3종(`architecture.html` / `system-architecture.html` / `aws-architecture.html`)
- 확정: 결제 원장엔 partner 없음 / 주문상품에서 partner / 복합결제(주2+보조2) / 결제수단별 차등수수료(method_fee, as-of) / 결제·주문 CDC·마스터 스냅샷 / settlement_event FREEZE(불변 원장) / 정산≠환불 / 정산은 상류를 projection으로 보유
- 메시지 버퍼 = **Kafka(KRaft)** 확정 (Redpanda 후보였으나 자바/운영 표준성 고려해 Kafka)
- DuckDB 컴퓨트: 도커 이미지 `settlement-duckdb:1.5.3`

---

## 🔵 단계 1 — 정산 배치 로직 검증 (DuckDB) ★ 현재

**목표**: 수집은 됐다 치고(가짜 데이터), 정산 배치(조인→배분→freeze→집계→대사)가 **설계대로 정확히** 도는지 실행으로 검증.

### 작업 (Tasks)
1. **스키마** `transform/01_schema.sql` — 마스터/설정(method_type·method_fee·partner·mid_map)·소스(payment·payment_method·order_item·cancel)·파생(settlement_event·settlement_agg·recon_result·recon_card·pg_settlement_file)  ✅
2. **시드 데이터** `generator/02_seed.sql`
   - 주문 10만건, 라인 1~3개
   - **스큐 주입**: musinsa 주문의 70% 라인 → partner_id=1
   - **복합결제**: payment(헤더) + payment_method(카드+머니 동시/단독, 포인트·쿠폰 보조)
   - PAY 전건 + CANCEL 5% (**전체+부분취소**, cancel 라인 명세)
   - `method_fee` **버전**: 카드 2026-06-01부터 0.025→0.023 (결제수단별 as-of 검증)
   - **재현성**: random() 비결정 차단(hash 배정 + 단일스레드) → 시드 결정적
   - event_time: 5/15~6/9 분포 (월 경계로 as-of 갈리게)
3. **정산 배치** `transform/03_settlement.sql`
   - **조인**: payment ⋈ order_item ON order_no, ⋈ payment_method ON txn_id
   - **교차배분**: ①라인 배분(상품가 비율) ②결제수단 배분(수단 비율), 각 단계 **잔액보정**(돈 보존)
   - **freeze**: method_fee **ASOF JOIN**(method별, effective_from ≤ 거래일) → fee_rate·fee·net 박제 → settlement_event(라인×수단)
   - **집계**: settlement_event → settlement_agg (멱등, (정산일+partner+brand+method))
   - **대사①**: 결제원천 PAY합 = 집계 gross합 → recon_result
   - **대사②**: 주결제수단 MID별 = PG파일 → recon_card
4. **검증 쿼리** `transform/04_checks.sql` — 아래 기준 출력
5. **실행** `scripts/run-step1.sh` — 도커 DuckDB로 01→02→03→04 파이프

### 검증 기준 (Acceptance) — ✅ 전부 통과 (v3, 2026-06-11)
- [x] **돈 보존**: 대사① `diff=0`, 결제원천=집계=16,489,637,984원 일치 (2단 잔액보정, 반올림 누수 0)
- [x] **차등수수료 freeze**: 결제수단별 as-of — 카드 5월 2.5%/6월 2.3%, 머니 1.5%, 포인트/쿠폰 0% (유효세율 카드 2.436%)
- [x] **복합결제**: payment_method 4종(주2+보조2), 카드+머니 동시결제, 결제수단별 백데이터
- [x] **부분취소**: cancel 라인 명세 → 취소 라인만 셀러 정산 차감
- [x] **MID 대사**: 대사② 주결제수단 MID별 `diff=0`
- [x] **멱등성**: 원장 고정·정산 재실행 → agg/event 체크섬 비트 동일
- [x] **재현성**: 시드 재생성 2회 → 6개 소스/파생 체크섬 전부 동일
- 실행: `bash scripts/run-step1.sh`

### 산출물
- 동작하는 정산 배치 SQL 4종 + 실행 스크립트 + 조회 도구(`scripts/q.sh`)
- 검증 통과 로그 (돈보존·차등수수료·복합결제·부분취소·MID대사·멱등·재현)

---

## ⬜ 단계 2 — 실시간 수집

**목표**: 1단계의 가짜 데이터를 **실제 소스 + CDC**로 대체. 결제·주문만 CDC.

### 작업
- `infra/docker-compose.yml`: Postgres(결제·주문, logical WAL) + Kafka(KRaft) + Debezium + (MinIO)
- `infra/postgres/`: 도메인별 스키마(결제/주문) + Outbox
- Debezium 커넥터 등록(결제·주문 각 1개) → Kafka 토픽
- 마스터(partner·mid_map)·설정(method_type·method_fee) = 스냅샷/시드 (CDC 아님)
- 컨슈머/싱크: Kafka → 레이크(또는 DuckDB 적재)

### 검증 기준
- [ ] Postgres에 결제·주문 INSERT → Kafka 토픽에 CDC 이벤트 도착
- [ ] 토픽 → 레이크 projection 적재 확인
- [ ] 1단계 배치가 "수집된" 데이터로 동일 결과

---

## ⬜ 단계 3 — 레이크 + 증분 멱등

**목표**: 야간 일괄 → **증분 마이크로배치**, 컬럼나 레이크.

### 작업
- MinIO + Iceberg 테이블(결제·주문 projection, settlement_event/agg)
- 1단계 SQL → **dbt-duckdb 모델**로 이식 (증분 materialization)
- watermark/오프셋 기반 증분, 멱등 upsert
- 미아(주문 미도착) → pending 재시도 → dead_letter

### 검증 기준
- [ ] 증분 실행: 새 데이터만 처리, 누적 동일
- [ ] 두 번 돌려도 settlement_agg 동일(멱등)
- [ ] 스키마 진화(컬럼 추가) 무중단

---

## ⬜ 단계 4 — 스큐·성능·스키마진화 검증

**목표**: 규모 키워 **실측** + 엣지 검증.

### 작업
- 데이터 100만→1,000만→(1억) 단계별 생성
- 처리시간·초당건수·메모리 계측(`bench/`)
- 핫파트너 스큐 → 서브파티셔닝 효과 측정
- 스키마 진화·부분취소(원거래 비례) 확장

### 검증 기준
- [ ] 규모별 처리시간 선형성/병목 식별
- [ ] 1억 외삽 추정치 산출
- [ ] 스큐 완화 효과 수치화

---

## ⬜ 단계 5 (옵션) — 오케스트레이션·관측·마감·지급

- Airflow DAG(수집→집계→대사→마감→지급), SLA
- 관측(처리량·랙·실패·불일치) 대시보드·알림
- 마감(동결·역분개) + payout 출력 자동화

---

## 현재 위치
**단계 1 완료(v3, 2026-06-11)** — 정산 배치 SQL 4종 동작·전 검증 통과(돈보존·차등수수료·복합결제·부분취소·MID대사·멱등·재현). 본 DB(`data/settlement.duckdb`) v3 반영 완료. 다음은 문서 다이어그램 동기화 또는 **단계 2(실시간 수집)**.
