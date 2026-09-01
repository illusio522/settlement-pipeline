# 스킴 확정서 — 복합결제 기반 파트너 정산 (v3)

> 원칙: **결제 원장엔 정산 대상(partner)이 없다.** partner는 주문상품에서 나오며,
> 정산 = `결제 ⋈ 주문상품` 조인 + 배분(allocation)으로 파생된다.
> **복합결제(복수 결제수단)**를 지원: 셀러 정산액은 상품가 기준이되, **백데이터·수수료는 결제수단별**로 쪼갠다.
> 금액은 전부 BIGINT(원, 정수). 코드값은 SMALLINT.
> 소스 SQL: `transform/01_schema.sql`(스키마) · `generator/02_seed.sql`(시드) · `transform/03_settlement.sql`(배치) · `transform/04_checks.sql`(검증).

코드 컨벤션:
- `type`: 1=PAY, 2=CANCEL  (※ REFUND는 정산 범위 밖 — 아래 "정산 ≠ 환불" 참고)
- `brand`: 1=mall-a, 2=mall-b
- `status`(대사): 0=MATCHED, 1=UNMATCHED
- `method`(결제수단): 1=CARD, 2=MONEY(주결제) · 3=POINT, 4=COUPON(보조) — `method_type`로 확장
- `category`: MAIN(주결제수단, 정산·PG대사 대상) / AUX(보조, 플랫폼 부담)

---

## Layer 0 — 마스터/설정

### `method_type` — 결제수단 마스터 (★ 확대 용이: 행 추가만)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| method_code | SMALLINT PK | 1=CARD 2=MONEY 3=POINT 4=COUPON |
| method_name | VARCHAR | 표시명 |
| category | VARCHAR | MAIN / AUX |
| is_pg | BOOLEAN | PG 정산·MID대사 대상 여부 (카드·머니=TRUE) |

> 주결제수단 확대 = `INSERT INTO method_type VALUES (5,'BANK','MAIN',TRUE)` 한 줄. 정산 로직은 수정 불필요.

### `method_fee` — 결제수단별 정산 수수료율 (★ 차등 수수료, 버전드 as-of)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| method | SMALLINT | 결제수단 코드 |
| fee_rate | DECIMAL(5,4) | 정산 수수료율 |
| effective_from | DATE | 시점별 적용(ASOF) |

> partner_contract(파트너별)가 아니라 **결제수단별**. 카드 2.5%(2026-06-01부터 2.3%)·머니 1.5%·포인트/쿠폰 0%.
> (향후 확장: partner×method 2차원 차등.)

### `partner` — 입점업체 마스터 (스냅샷)
| 컬럼 | 타입 |
|---|---|
| partner_id | INTEGER PK |
| name | VARCHAR |
| brand | SMALLINT |

### `mid_map` — 페이먼츠 MID ↔ brand (스냅샷)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| payments_mid | INTEGER PK | 주결제수단(카드·머니)별 MID |
| brand | SMALLINT | 소속 몰 |

> 브랜드당 MID 2개 이상(PG사·결제수단·법인·정산주기별). `pg_mid`(외부 PG MID)는 PG 3자대사 붙일 때 추가.

---

## Layer 1 — 소스 (결제·주문 도메인, raw)

### `payment` — 결제 헤더 (★ partner·payments_mid 없음)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| txn_id | BIGINT PK | 거래/승인 ID |
| order_no | BIGINT | 주문번호 (조인 키) |
| brand | SMALLINT | 몰 |
| amount | BIGINT | 결제 **총액**(원) |
| type | SMALLINT | 1=PAY, 2=CANCEL |
| orig_txn_id | BIGINT NULL | 취소의 원거래 참조 |
| event_time | TIMESTAMP | 거래시각 (→ settlement_date) |

> payment는 **결제 헤더(주문 단위 총액)**. 결제수단별 내역은 `payment_method`로 분리(복합결제).

### `payment_method` — 결제수단별 승인/취소 (★ 복합결제)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| pm_id | BIGINT PK | 결제수단 라인 ID |
| txn_id | BIGINT | 헤더 결제 참조 |
| order_no | BIGINT | 주문번호 |
| method | SMALLINT | 1=CARD 2=MONEY 3=POINT 4=COUPON |
| payments_mid | INTEGER NULL | 주결제수단(카드·머니)만 MID 보유, 보조는 NULL |
| amount | BIGINT | 해당 수단 결제액 |
| type | SMALLINT | 1=PAY, 2=CANCEL |
| event_time | TIMESTAMP | |

> 한 주문 = payment 1행 + payment_method N행(카드+머니+포인트+쿠폰). 합 = payment.amount.

### `order_item` — 주문상품 (★ partner 등장)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| order_no | BIGINT | 조인 키 |
| line_no | SMALLINT | 주문 내 라인번호 |
| partner_id | INTEGER | 입점업체(정산 대상) |
| brand | SMALLINT | |
| item_amount | BIGINT | 상품 금액 |
| (논리 PK) | (order_no, line_no) | |

### `cancel` — 취소 라인 명세 (★ 부분취소)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| cancel_txn_id | BIGINT | 취소 거래 ID |
| order_no | BIGINT | 원주문 |
| line_no | SMALLINT | 취소된 라인 (일부만 = 부분취소) |

> 셀러 정산에서 **취소된 라인만 차감**. 전체취소=전 라인, 부분취소=일부 라인.

---

## Layer 2 — 정산 이벤트 (파생: 조인+교차배분+freeze, canonical 불변 원장)

### `settlement_event` — ★ grain = (주문라인 × 결제수단)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| event_id | VARCHAR | 멱등 dedup 키 = `brand:txn_id:line_no:method` |
| txn_id | BIGINT | 원천 결제 참조 |
| order_no | BIGINT | |
| line_no | SMALLINT | |
| partner_id | INTEGER | 주문상품에서 귀속 |
| brand | SMALLINT | |
| type | SMALLINT | 1=PAY, 2=CANCEL |
| method | SMALLINT | 결제수단 |
| payments_mid | INTEGER NULL | 주결제수단 MID |
| allocated_amount | BIGINT | (라인배분 × 결제수단비율) 교차배분액 |
| fee_rate | DECIMAL(5,4) | **거래시점 결제수단별** as-of 수수료(박제) |
| fee | BIGINT | allocated × fee_rate (박제) |
| net_amount | BIGINT | allocated − fee (박제) |
| event_time | TIMESTAMP | |
| settlement_date | DATE | date(event_time) |

> **교차배분**: 결제총액 → ①라인 비례 배분 → ②각 라인 안에서 결제수단 비율로 재배분. 각 단계 **잔액보정**으로 돈 보존(반올림 누수 0).
> freeze: 거래시점 수수료를 박제 → 불변·재현·감사. 재집계해도 동일.

---

## Layer 3 — 집계 (멱등 재생성)

### `settlement_agg` — grain = (정산일 × 파트너 × 브랜드 × 결제수단)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| settlement_date | DATE | |
| partner_id | INTEGER | |
| brand | SMALLINT | |
| method | SMALLINT | ★ 결제수단별 명세 |
| gross | BIGINT | PAY 배분액 합 |
| cancel | BIGINT | CANCEL 배분액 합 |
| net | BIGINT | gross − cancel |
| fee | BIGINT | PAY 수수료 합 |
| payout | BIGINT | net − fee (지급액) |
| txn_count | BIGINT | 이벤트 수 |

> 셀러 **지급액은 상품가 기준 총액**(돈은 합산), **백데이터/수수료는 결제수단별**로 제공.

---

## Layer 4 — 대사 (Reconciliation) ★ 2종

### `pg_settlement_file` — PG가 준 정산내역 (대사 소스, MID 단위)
| 컬럼 | 타입 |
|---|---|
| settlement_date | DATE |
| payments_mid | INTEGER |
| pg_amount | BIGINT |

### `recon_result` — 대사①: 파트너 돈 보존 (결제원천 = 집계)
| 컬럼 | 타입 | 설명 |
|---|---|---|
| settlement_date | DATE | |
| brand | SMALLINT | |
| src_pay | BIGINT | 결제원천 PAY 합 |
| agg_pay | BIGINT | 집계 gross 합 |
| diff | BIGINT | 0이어야 |
| status | SMALLINT | 0=MATCHED |

### `recon_card` — 대사②: 주결제수단 MID별 PG 대사
| 컬럼 | 타입 | 설명 |
|---|---|---|
| settlement_date | DATE | |
| payments_mid | INTEGER | 카드·머니 MID |
| card_total | BIGINT | 내부 결제수단 합(MID별) |
| pg_total | BIGINT | PG 파일 합 |
| diff | BIGINT | 0이어야 |
| status | SMALLINT | 0=MATCHED |

> 돈 보존(파트너)과 PG 대사(주결제수단 MID)는 **별개 축**. 보조결제(포인트·쿠폰)는 PG 대사 대상 아님(`is_pg=FALSE`).

---

## 정산 ≠ 환불 (★ 범위 확정)

- **정산 = 결제·주문 → 파트너 지급액 산출.** 취소는 셀러 정산에서 **차감**까지만 다룬다.
- **환불(자금 정책·환불 대사, 원결제와 다른 수단으로의 환급 등)은 정산 범위 밖** → 본 파이프라인 미구현.
- 그래서 `type`에 REFUND 없음. 향후 별도 자금/환불 도메인.

---

## 향후 레이어 (미구현 — 2단계+)

| 레이어 | 테이블(예정) | 비고 |
|---|---|---|
| 마감(Close) | settlement_close(date, status, control_total, closed_at) | 동결·역분개 |
| 지급(Payout) | payout(date, partner_id, amount, status) | 지급 출력 |
| 운영 | dead_letter / processed_offset | 미아 격리·증분 watermark |

---

## 데이터 수집 방식 (★ 모든 걸 CDC로 긁지 않음)
| 데이터 | 방식 | 이유 |
|---|---|---|
| payment / payment_method | **CDC** | 이벤트성·대량·실시간 |
| order_item / cancel | **CDC** | 이벤트성·대량 |
| partner / mid_map | **스냅샷(일 1회)** | 마스터·소량·거의 불변 |
| method_type / method_fee | **정산이 직접 보유** | 정산 도메인 설정 |

> 정산은 결제·주문의 **필요 컬럼만 읽기전용 projection**으로 보유. 진실의 소유는 상류 도메인. (실 구현은 2단계.)

## 매핑 체인
```
payment(헤더,총액) ─order_no─► order_item.partner_id ─► partner
payment ─txn_id─► payment_method(수단별,MID) ─payments_mid─► mid_map ─► brand / PG대사
order_item.partner_id + method ─► method_fee(as-of 차등수수료)
```

## 데이터 흐름 요약
```
payment ⋈ order_item ⋈ payment_method
  --라인배분→수단교차배분→freeze(결제수단별 as-of 수수료)-->  settlement_event(라인×수단, 불변)
  --멱등 집계-->  settlement_agg(파트너×수단)
  --대사①돈보존(파트너)-->  recon_result
  --대사②MID(주결제수단)-->  recon_card
```
