# 데이터 레이아웃 (1단계 v3 구현 기준)

> 실제 구현된 테이블·키·인코딩·활용 정리. 소스 SQL: `transform/01_schema.sql`(스키마), `generator/02_seed.sql`(시드), `transform/03_settlement.sql`(배치), `transform/04_checks.sql`(검증).
> 코드값: `type` 1=PAY 2=CANCEL · `brand` 1=mall-a 2=mall-b · `method` 1=CARD 2=MONEY 3=POINT 4=COUPON · `status` 0=MATCHED 1=UNMATCHED · 금액=BIGINT(원)

---

## 1. 테이블 분류

| 구분 | 테이블 | 역할 |
|---|---|---|
| **소스(입력)** | payment | 결제 헤더 (주문 총액, partner·MID 없음) |
| | payment_method | 결제수단별 승인/취소 (복합결제, MID 보유) |
| | order_item | 주문상품(라인) — partner 귀속 |
| | cancel | 취소 라인 명세 — 부분취소 |
| **마스터/설정** | method_type | 결제수단 마스터 (주/보조, is_pg) |
| | method_fee | 결제수단별 수수료율 (버전드 as-of) |
| | partner | 입점업체 식별 |
| | mid_map | payments_mid ↔ brand |
| | pg_settlement_file | PG 정산내역(대사 소스) |
| **파생(출력)** | settlement_event | (라인×결제수단) 배분·freeze (불변 원장) |
| | settlement_agg | (파트너×결제수단) 집계 (지급액·백데이터) |
| | recon_result | 대사① 파트너 돈 보존 |
| | recon_card | 대사② 주결제수단 MID별 PG 대사 |

---

## 2. 테이블별 컬럼 (핵심)

### payment (소스 · 결제 헤더)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| txn_id | BIGINT PK | 거래 ID (인코딩 아래) |
| order_no | BIGINT | 주문번호 (조인 키) |
| brand | SMALLINT | 몰 |
| amount | BIGINT | 결제 **총액** |
| type | SMALLINT | 1=PAY 2=CANCEL |
| orig_txn_id | BIGINT | 취소의 원거래 |
| event_time | TIMESTAMP | 거래시각 (→ settlement_date) |

### payment_method (소스 · 결제수단별 — 복합결제)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| pm_id | BIGINT PK | 결제수단 라인 ID (= txn_id*10 + 수단코드) |
| txn_id | BIGINT | 헤더 참조 |
| order_no | BIGINT | |
| method | SMALLINT | 1=CARD 2=MONEY 3=POINT 4=COUPON |
| payments_mid | INTEGER | ★ 주결제수단만(카드·머니), 보조=NULL |
| amount | BIGINT | 해당 수단 결제액 |
| type | SMALLINT | 1=PAY 2=CANCEL |
| event_time | TIMESTAMP | |

> 한 주문 = payment 1행 + payment_method N행. Σamount = payment.amount.

### order_item (소스 · 주문상품/라인)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| order_no | BIGINT | 조인 키 |
| line_no | SMALLINT | 주문 내 상품 순번 |
| partner_id | INTEGER | ★ 입점업체(정산 대상) |
| brand | SMALLINT | 몰 |
| item_amount | BIGINT | 상품 금액 |
| (논리 PK) | (order_no, line_no) | |

### cancel (소스 · 취소 라인 명세)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| cancel_txn_id | BIGINT | 취소 거래 ID (= 1e8 + order_no) |
| order_no | BIGINT | 원주문 |
| line_no | SMALLINT | 취소된 라인 (일부만 = 부분취소) |

### method_type / method_fee / partner / mid_map (마스터/설정)
```
method_type : method_code(PK), method_name, category(MAIN/AUX), is_pg
method_fee  : method, fee_rate, effective_from        -- 결제수단별 버전드(as-of)
partner     : partner_id(PK), name, brand
mid_map     : payments_mid(PK), brand                 -- pg_mid는 추후
```

### settlement_event (파생 · 불변 원장)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| event_id | VARCHAR | (라인×수단) 유일 ID = `brand:txn_id:line_no:method` |
| txn_id, order_no, line_no | | 원천 참조 |
| partner_id, brand, type | | |
| method | SMALLINT | 결제수단 |
| payments_mid | INTEGER | 주결제수단 MID |
| allocated_amount | BIGINT | 교차배분액 (박제) |
| fee_rate | DECIMAL(5,4) | 결제수단별 as-of 수수료 (박제) |
| fee | BIGINT | allocated×fee_rate (박제) |
| net_amount | BIGINT | allocated−fee (박제) |
| event_time, settlement_date | | |
| 입자(grain) | **(주문라인 × 결제수단) = 1행** | |

### settlement_agg (파생 · 집계)
| 컬럼 | 설명 |
|---|---|
| (settlement_date, partner_id, brand, method) | 논리 PK |
| gross / cancel | type별 배분액 합 |
| net | gross − cancel |
| fee | PAY 수수료 합 |
| payout | net − fee (지급액) |
| txn_count | 이벤트 수 |
| 입자 | **(정산일+파트너+브랜드+결제수단) = 1행** |

### recon_result (대사① 돈 보존) / recon_card (대사② MID)
```
recon_result : (settlement_date, brand) | src_pay, agg_pay, diff(=0), status
recon_card   : (settlement_date, payments_mid) | card_total, pg_total, diff(=0), status
```

---

## 3. 키 조합 · 인코딩 (★ 중요)

### event_id — `brand : txn_id : line_no : method`
```sql
brand || ':' || txn_id || ':' || line_no || ':' || method
```
- 예: `2:5:1:1` = 몰B, 거래5, 라인1, 카드
- (라인×결제수단) 1건을 유일 식별 → **멱등 dedup 키** + 추적 키
- v1 대비 `:method` 추가 (복합결제로 grain이 결제수단까지 내려감)

### txn_id — type 인코딩
| 거래 | txn_id |
|---|---|
| PAY | `order_no` |
| CANCEL | `100000000 + order_no` |
- CANCEL.orig_txn_id = 원 PAY txn_id(=order_no)

### pm_id — payment_method 라인 ID
```sql
txn_id*10 + 수단코드   -- 1=card 2=money 3=point 4=coupon
```

### allocated_amount (★ 교차배분 — 2단)
```sql
-- ① 라인 배분: 결제총액을 상품가 비율로
line_alloc = ROUND(payment.amount × item_amount / 주문라인합)
             마지막 라인 = payment.amount − (다른 라인 합)        -- 잔액보정

-- ② 결제수단 배분: 각 라인 안에서 결제수단 비율로
method_alloc = ROUND(line_alloc × 수단금액 / 주문수단합)
               마지막 수단 = line_alloc − (다른 수단 합)          -- 잔액보정
```
- 2단 모두 잔액보정 → 반올림 누수 0 (돈 보존)

### fee_rate (freeze, as-of · 결제수단별)
```sql
ASOF JOIN method_fee f ON f.method = d.method AND d.settlement_date >= f.effective_from
-- 결제수단별 effective_from ≤ 거래일 중 가장 최근 수수료를 박제
```
- 카드 2.5%(2026-06-01부터 2.3%) · 머니 1.5% · 포인트/쿠폰 0%
- fee = ROUND(allocated × fee_rate), net = allocated − fee

---

## 4. 관계 (조인 키)

```
payment.order_no ──────────► order_item.order_no        (결제 ⋈ 주문, 라인배분)
payment.txn_id ────────────► payment_method.txn_id       (헤더 ⋈ 수단, 교차배분)
order_item.partner_id ─────► partner.partner_id          (파트너 메타)
(method, settlement_date) ─► method_fee                  (as-of 차등수수료)
payment_method.payments_mid► mid_map.payments_mid        (→ brand, MID 대사)

settlement_event ──(집계)──► settlement_agg
payment + settlement_agg ──(대사①)──► recon_result
payment_method + pg_file ──(대사②)──► recon_card
```

---

## 5. 활용

| 키/테이블 | 활용 |
|---|---|
| **event_id** | ① 멱등 dedup(증분 적재) ② 추적(agg→event 드릴다운) ③ 분쟁/감사 증빙 |
| **settlement_event** | 불변 원장 — 재현성·감사의 진실. 재집계해도 동일 |
| **settlement_agg** | 최종 정산(파트너별 지급액) + **결제수단별 백데이터** |
| **recon_result** | 파트너 돈 보존 검산 (diff=0) |
| **recon_card** | 주결제수단 MID별 PG 대사 (diff=0) |
| 파생 4종 | 배치 재실행 시 TRUNCATE+재계산 → **멱등** |

---

## 6. 재현성 (시드 결정성) ★

DuckDB `random()` 2종 비결정을 차단해 **돌릴 때마다 같은 fake 데이터** 보장:
- **① 가변 호출**(한 행서 random 호출수가 분기마다 다름) — `partner_id`·`cancel kind` → 결정적 `hash(키)`로 교체. `threads=1`로도 안 잡힘.
- **② 고정 호출이나 저장테이블 morsel 병렬 스캔서 흔들림** — `payment_method` 등 → 시드 구간 `SET threads TO 1`(정산 03은 `RESET threads`로 병렬).
- 검증: 풀파이프 2회 oi/pay/cancel/pm/event/agg 6개 체크섬 전부 동일.
- ※ **멱등성**(원장 고정·정산만 재실행 → 동일)과 **재현성**(시드 재생성 → 동일)은 별개. `run-step1.sh`는 원장까지 새로 만드니 멱등성 검증용 아님.

---

## 7. 1단계 단순화 (운영 시 정제 대상)
- `event_id` 문자열 조합 → 운영은 복합키/UUID 권장
- `txn_id`에 1e8 더해 type 구분 / `pm_id = txn_id*10+코드` → 운영은 명시적 ID 체계
- `mid_map`에 `pg_mid` 없음 → PG 3자대사 시 추가
- 배분 = item_amount 비율(할인 없음) → 운영은 할인·배송비 안분
- 차등수수료 = 결제수단별 → 향후 partner×method 2차원
- 정산 ≠ 환불: 환불 대사는 범위 밖 (취소는 셀러 정산 차감만)
