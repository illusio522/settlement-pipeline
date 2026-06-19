# 최종 설계안 — 대규모 파트너 정산 파이프라인

## 0. 목표 (4대 비기능)
**고성능 · 저비용 · 빠른 처리속도 · 유지보수**
핵심 레버: `증분 · 컬럼나 · 멱등 · 버스트컴퓨트 · 선언적/표준`

## 1. 아키텍처

```
[무신사 결제DB]   [29cm 결제DB]
   (+Outbox)        (+Outbox)
   WAL│              WAL│
 ┌────┴────────────────┴────┐
 │  CDC (Debezium)          │  로그 테일링 — 소스 부하 ≈ 0
 └────────────┬─────────────┘
              │ 정산 이벤트
        [Kafka(KRaft)]  경량 로그(버퍼·디커플·재처리)
              │
        [정규화]  브랜드별 → canonical (정산 경계 = 엄격 검증)
              │
        [MinIO/S3 + Iceberg]  컬럼나·날짜파티션 랜딩 (schema-on-read)
              │  5~15분 트리거
        [증분 마이크로배치 (dbt + DuckDB)]  (정산일+파트너+브랜드) 멱등 upsert
              │
        [집계 스토어 + append-only 원장]
              │  컷오프
        [확정] 수수료·세금 → 지급액
              │
        [대사] PG파일 ↔ 원장 ↔ 브랜드 (3자 컨트롤토탈)
              │
        [마감(동결·불변)] → [지급]

  횡단: [관측·SLA알림] [Airflow DAG] [데드레터→재처리] [데이터품질테스트]
```

## 2. 데이터 플로우

| # | 단계 | 선택 | 역할 |
|---|---|---|---|
| 1 | 수집 | Debezium CDC + Outbox | 로그 테일링, 내부스키마↔이벤트 디커플 |
| 2 | 버퍼 | Kafka(KRaft) | 경량 로그, 디커플·재처리 |
| 3 | 정규화 | canonical 검증 | 브랜드차이 흡수, 정산 경계 엄격검증 |
| 4 | 랜딩 | MinIO + Iceberg/Parquet | 컬럼나·파티션·스키마진화 |
| 5 | 집계 | dbt + DuckDB 증분 | (정산일+파트너+브랜드) 멱등 upsert |
| 6 | 확정 | 수수료·세금 | 파트너별 지급액 |
| 7 | 대사 | 3자 컨트롤토탈 | 불일치 자동탐지 |
| 8 | 마감 | 동결·역분개 | 불변·감사 |

## 3. 4목표 매핑

| 목표 | 기여 |
|---|---|
| ⚡ 고성능 | CDC 로그테일링 / 컬럼나+프루닝 / 파티션병렬 / 합계우선 대사 |
| 💰 저비용 | 오브젝트스토리지 / 증분 / 버스트컴퓨트 / 멱등재처리 / Kafka(KRaft)·DuckDB 단일노드 |
| 🚀 빠름 | 실시간 CDC / 증분 피크평탄화 / 스큐제거 / 합계우선 대사 |
| 🔧 유지보수 | Outbox 디커플 / Iceberg 진화 / dbt 선언적+테스트+리니지 / 멱등·replay / 관측·DAG / 표준도구 |

## 4. 데이터 모델 (v3 — 복합결제·결제수단별)

> ★ partner는 결제 원장에 없음 → 주문상품에서 귀속. 셀러 지급액=상품가 기준, 백데이터·수수료=결제수단별.

```
소스(결제·주문, CDC)
  payment        : txn_id(PK) · order_no · brand · amount(총액) · type(PAY|CANCEL) · event_time
  payment_method : pm_id(PK) · txn_id · method(CARD|MONEY|POINT|COUPON) · payments_mid · amount · type
  order_item     : (order_no, line_no) · partner_id · item_amount
  cancel         : cancel_txn_id · order_no · line_no            -- 부분취소(라인 명세)

설정: method_type(주/보조,is_pg) · method_fee(method, fee_rate, effective_from)  -- 차등수수료 as-of

settlement_event (불변 원장 — freeze)
  event_id(PK = brand:txn:line:method) · partner_id · method · payments_mid
  allocated_amount · fee_rate(박제) · fee · net_amount · settlement_date
  grain = (주문라인 × 결제수단)

settlement_agg (집계 — 멱등 재생성)
  (settlement_date, partner_id, brand, method) [복합 PK]
  gross · cancel · net · fee · payout · txn_count

대사: recon_result(파트너 돈보존) · recon_card(주결제수단 MID별 PG대사)
※ 마감(settlement_close)·지급(payout)·환불 대사 = 향후/범위 밖
```

## 5. 정합성
- **멱등/정확히 한 번**: event_id dedup + 멱등 upsert + 오프셋 원자커밋. 누적 += 금지
- **대사**: 브랜드합 = 집계합 = PG파일합(3자). 파티션 체크섬으로 불일치 구간만 상세
- **마감**: 동결 후 불변, 정정은 역분개(조정 엔트리)
- **늦은 이벤트**: event-time 기준, 유예창 밖이면 다음 정산일 이월

## 6. 스키마 진화
- 유연한 랜딩(Iceberg mergeSchema) + 정산 경계 canonical 검증(엄격 코어)
- 추가형(nullable)만 허용, rename·타입변경은 단계적 이행
- Outbox로 브랜드 내부변경 격리 / 소급 시 콜드 레이크 멱등 재집계

## 7. 규모 & 성능
**운영(실측 대상)**: 입력 1억+/일, 파트너 수천~. 평상시 마감 초~수분, 전량 재처리 단일노드 수분. 병목=핫파트너 스큐 → 서브파티셔닝.
**로컬 테스트**: 브랜드2/파트너100(무신사 70% 스큐)/결제 100만~1,000만. 검증=멱등·정확히한번·대사·스키마진화·스큐.
**측정 계획**: 100만→1,000만→1억 단계별(시간·초당건수·메모리), 선형성·병목 식별, 운영급 노드+실제 스큐로 외삽. 각 단계 계측 훅 내장.

## 8. 로컬 스택 (린)
Postgres(+Outbox) · Debezium · Kafka(KRaft) · MinIO · Iceberg/Parquet · DuckDB+dbt · 데이터생성기(스큐) · 계측.
Spark·대형 Kafka = 과설계 → 미도입.

## 9. 확장 경로
단일노드(DuckDB) → 파티션병렬 → 분산(Spark/스트리밍, K8s). 멱등·재시작·외부호출격리는 1단계부터 내장.

## 10. 철학
> 전량 재처리·상시 인프라·자체 코드를 없애고, 증분·컬럼나·멱등·표준/선언적으로 운영을 단순하게.
> 규모에 맞게 right-sizing하되 구조는 확장 가능하게.
