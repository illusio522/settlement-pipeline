# ERD — 소스(Postgres) & 정산 canonical(DuckDB)

> Mermaid 렌더링: GitHub / VSCode(Markdown Preview Mermaid) / Obsidian 등.
> ★ **물리 FK 없음** (고트래픽 결제 원장 관행 — 락경합·샤딩·JPA 편의). 모든 연결 = 논리(점선).
> 무결성: 쓰기=앱 트랜잭션(헤더+디테일 같은 Tx) / 사후=정산 대사(고아·Σ수단=헤더 검증).

---

## 1. 2단계 소스 ERD — Postgres (paydb / orderdb)

★ 결제 도메인 = 헤더-디테일 원장: `payment`(마스터, 총액) ← 수단별 원장 4개(txn_id 논리 FK).
★ 도메인 내·도메인 간 모두 물리 FK 없음 — 논리 연결, 정합성은 앱 Tx + 정산 대사가 보장.

```mermaid
erDiagram
    %% ===== paydb (결제 도메인) — 전부 논리 연결 =====
    payment ||..o{ payment_card   : "txn_id (논리)"
    payment ||..o{ payment_money  : "txn_id (논리)"
    payment ||..o{ payment_point  : "txn_id (논리)"
    payment ||..o{ payment_coupon : "txn_id (논리)"
    payment |o..o{ payment        : "orig_txn_id (취소→원거래)"

    %% ===== 도메인 간 논리 연결 (FK 없음) =====
    payment    }o..o{ order_item : "order_no (논리)"
    order_item ||..o{ cancel     : "order_no+line_no (논리)"

    payment {
        bigint      txn_id      PK  "거래 ID"
        bigint      order_no        "주문번호(교차 도메인 키)"
        smallint    brand           "1=musinsa 2=29cm"
        bigint      amount          "결제 총액(원)"
        char3       currency        "KRW"
        smallint    type            "1=PAY 2=CANCEL"
        bigint      orig_txn_id     "취소의 원거래(논리)"
        smallint    status          "1=APPROVED 2=CANCELED 9=FAILED"
        timestamp   event_time      "거래시각(정산 기준)"
        timestamptz created_at      ""
        timestamptz updated_at      ""
    }
    payment_card {
        bigint    pm_id              PK ""
        bigint    txn_id                "헤더 참조(논리 FK)"
        bigint    order_no              ""
        integer   payments_mid          "페이먼츠 MID"
        bigint    amount                ""
        smallint  type                  ""
        varchar   card_company          "카드사"
        varchar   pg_txn_id             "PG 거래번호"
        varchar   approval_no           "승인번호"
        smallint  installment_months    "할부(0=일시불)"
        timestamp event_time            ""
    }
    payment_money {
        bigint    pm_id          PK ""
        bigint    txn_id         "논리 FK"
        bigint    order_no          ""
        integer   payments_mid      "페이먼츠 MID"
        bigint    amount            ""
        smallint  type              ""
        bigint    wallet_id         "머니 지갑"
        varchar   balance_txn_id    "잔액 거래번호"
        timestamp event_time        ""
    }
    payment_point {
        bigint    pm_id           PK ""
        bigint    txn_id          "논리 FK"
        bigint    order_no           ""
        bigint    amount             ""
        smallint  type               ""
        bigint    point_wallet_id    "포인트 지갑 (MID 없음)"
        timestamp event_time         ""
    }
    payment_coupon {
        bigint    pm_id      PK ""
        bigint    txn_id     "논리 FK"
        bigint    order_no      ""
        bigint    amount        ""
        smallint  type          ""
        bigint    coupon_id     "사용 쿠폰 (MID 없음)"
        bigint    policy_id     "쿠폰 정책"
        timestamp event_time    ""
    }

    %% ===== orderdb (주문 도메인) =====
    order_item {
        bigint      order_no        PK "복합 PK 1"
        smallint    line_no         PK "복합 PK 2"
        bigint      product_id         "상품"
        integer     qty                "수량"
        bigint      unit_price         "단가"
        bigint      item_amount        "qty x unit_price (정산 라인 금액)"
        bigint      discount_amount    "할인(안분 확장용)"
        integer     partner_id         "★ 입점업체(정산 대상)"
        smallint    brand              ""
        smallint    status             "1=ORDERED 2=CANCELED"
        timestamptz created_at         ""
    }
    cancel {
        bigint      cancel_txn_id PK "복합 PK 1"
        bigint      order_no      PK "복합 PK 2"
        smallint    line_no       PK "복합 PK 3"
        smallint    reason_code      "1=변심 2=품절 3=불량"
        timestamptz created_at       ""
    }
```

CDC publication: `dbz_pay`(payment + 4원장) · `dbz_order`(order_item, cancel)

---

## 2. 1단계 정산 canonical ERD — DuckDB (`transform/01_schema.sql`)

★ 소스의 수단별 4원장은 싱크에서 **UNION+정규화 → canonical `payment_method`** 로 통합된다(method 이름→SMALLINT 코드).
★ DuckDB라 물리 FK 없음 — 전부 논리 관계.

```mermaid
erDiagram
    %% 소스 projection (canonical)
    payment        ||..o{ payment_method   : "txn_id (논리)"
    payment        }o..o{ order_item       : "order_no (논리)"
    order_item     ||..o{ cancel           : "order_no+line_no (논리)"
    method_type    ||..o{ payment_method   : "method"
    mid_map        ||..o{ payment_method   : "payments_mid"
    partner        ||..o{ order_item       : "partner_id"

    %% 정산 파생
    payment_method ||..o{ settlement_event : "교차배분"
    order_item     ||..o{ settlement_event : "라인배분+partner귀속"
    method_fee     ||..o{ settlement_event : "ASOF (method, 거래일)"
    settlement_event ||..o{ settlement_agg : "GROUP BY 일·파트너·브랜드·수단"
    settlement_agg ||..o{ recon_result     : "대사1 돈보존"
    pg_settlement_file ||..o{ recon_card   : "대사2 MID"

    method_type {
        smallint method_code PK "1=CARD 2=MONEY 3=POINT 4=COUPON"
        varchar  method_name    ""
        varchar  category       "MAIN | AUX"
        boolean  is_pg          "PG 정산·MID대사 대상"
    }
    method_fee {
        smallint method         "결제수단"
        decimal  fee_rate       "차등 수수료"
        date     effective_from "as-of 버전"
    }
    payment_method {
        bigint   pm_id        PK "canonical (4원장 UNION)"
        bigint   txn_id          ""
        smallint method          "이름→코드 정규화"
        integer  payments_mid    "주결제수단만"
        bigint   amount          ""
        smallint type            ""
    }
    settlement_event {
        varchar  event_id         PK "brand:txn:line:method (멱등 dedup)"
        integer  partner_id          ""
        smallint method              ""
        bigint   allocated_amount    "교차배분액(박제)"
        decimal  fee_rate            "as-of 수수료(박제)"
        bigint   fee                 "(박제)"
        bigint   net_amount          "(박제)"
        date     settlement_date     ""
    }
    settlement_agg {
        date     settlement_date PK "복합 PK"
        integer  partner_id      PK ""
        smallint brand           PK ""
        smallint method          PK "★ 결제수단별 명세"
        bigint   gross              ""
        bigint   cancel             ""
        bigint   fee                ""
        bigint   payout             "지급액"
    }
    recon_result {
        date     settlement_date "" 
        smallint brand    ""
        bigint   diff     "0이어야"
        smallint status   "0=MATCHED"
    }
    recon_card {
        date     settlement_date ""
        integer  payments_mid    ""
        bigint   diff            "0이어야"
        smallint status          "0=MATCHED"
    }
    partner {
        integer  partner_id PK ""
        varchar  name          ""
        smallint brand         ""
    }
    mid_map {
        integer  payments_mid PK ""
        smallint brand           ""
    }
    pg_settlement_file {
        date    settlement_date ""
        integer payments_mid    ""
        bigint  pg_amount       ""
    }
    order_item {
        bigint   order_no    PK ""
        smallint line_no     PK ""
        integer  partner_id     ""
        bigint   item_amount    ""
    }
    payment {
        bigint   txn_id   PK ""
        bigint   order_no    ""
        bigint   amount      "총액"
        smallint type        ""
    }
    cancel {
        bigint   cancel_txn_id PK ""
        bigint   order_no      PK ""
        smallint line_no       PK ""
    }
```

---

## 3. 소스 → canonical 매핑 요약

| 소스(Postgres) | canonical(DuckDB) | 변환 |
|---|---|---|
| payment | payment | projection (currency·status·audit 컬럼 제외, status=APPROVED 필터) |
| payment_card | payment_method | UNION + `method='CARD'→1` |
| payment_money | payment_method | UNION + `method='MONEY'→2` |
| payment_point | payment_method | UNION + `method='POINT'→3` (MID NULL) |
| payment_coupon | payment_method | UNION + `method='COUPON'→4` (MID NULL) |
| order_item | order_item | projection (product·qty·discount 등 제외) |
| cancel | cancel | projection (reason_code 제외) |
| (없음 — 정산 소유) | method_type·method_fee·partner·mid_map | 스냅샷/시드 (CDC 아님) |
