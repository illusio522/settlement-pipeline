# 파일 기반 정산 실행·검증

`feat/file-batch`는 기존 CDC 코드를 유지하면서 파일 입력 경로를 추가한 브랜치다. 기존 구현 기준점은 `archive/cdc-lakehouse`다. 기본 실행은 PostgreSQL → Parquet bundle → DuckDB/dbt → 정합성 검사 → PostgreSQL 날짜별 게시이며, Java 조회 API의 기존 스키마를 유지한다.

## 준비

Python 3.11 이상, Docker, Java 21을 사용한다. Python 배치 의존성은 검증한 전체 버전을 `file_batch/requirements.lock`에 고정했다.

```bash
python3.11 -m venv .venv-file-batch
.venv-file-batch/bin/pip install -r file_batch/requirements.lock
export FILE_BATCH_PYTHON="$PWD/.venv-file-batch/bin/python"
```

로컬 최소 인프라는 아래 Compose다. 기존 `infra/docker-compose.yml`과 프로젝트명·포트·볼륨을 분리한다.

```bash
# POSTGRES_USER / POSTGRES_PASSWORD는 환경에서 주입한다.
docker compose -f infra/file-batch/compose.yml up -d --wait postgres
```

기본 호스트 포트는 `55432`다. `paydb`, `orderdb`, `servedb`를 생성한다. 소스 데이터는 실제 상류 또는 기존 합성 데이터 적재 절차로 준비한다. 기존 `scripts/e2e-load-postgres.sh`는 기존 CDC 컨테이너를 대상으로 하므로 새 환경에서 그대로 실행하지 않는다.

호스트 CLI의 접속 정보는 `FILE_BATCH_PAY_DSN`, `FILE_BATCH_ORDER_DSN`, `FILE_BATCH_SERVING_DSN`으로 주입한다. DSN에 비밀번호를 포함해야 하는 경우 셸 이력·코드에 저장하지 않는다. 컨테이너로 배치를 실행할 때는 DSN 호스트를 Compose 서비스 이름 `postgres`, 포트를 `5432`로 설정한다.

```bash
"$FILE_BATCH_PYTHON" -m file_batch init
```

조회 앱의 `SPRING_DATASOURCE_URL`은 새 `servedb` 접속 주소로 지정하고, 기존 사용자·비밀번호·`SERVING_API_KEY` 환경변수를 주입한다.

## 1. 기준정보와 상류 마감 명세

`masters.json`은 다음 구조다. 수수료는 소수 문자열로 입력한다.

```json
{
  "version": "master-v1",
  "partner": [{"partner_id": 1, "name": "partner-1", "brand": 1}],
  "method_fee": [{"method": 1, "fee_rate": "0.0250", "effective_from": "2020-01-01"}],
  "mid_map": [{"payments_mid": 1001, "brand": 1}]
}
```

`close.json`은 상류가 마감을 완료했다고 확인한 기준이다. 아래 값은 형식 예시이며 실제 대상 건수·금액으로 작성한다. 파일 추출이 성공했다는 사실만으로 상류의 마감 완료를 대신하지 않는다.

```json
{
  "ready": true,
  "reference": "upstream-close-2026-05-20-v1",
  "settlement_date": "2026-05-20",
  "expected": {"pay_count": 1, "cancel_count": 0, "gross": 10000, "cancel": 0}
}
```

## 2. 불변 입력 파일 추출

```bash
"$FILE_BATCH_PYTHON" -m file_batch export \
  --date 2026-05-20 --root data/file-batch/input \
  --masters data/file-batch/masters.json --close data/file-batch/close.json
```

반환된 `bundle` 경로를 다음 실행에 사용한다. 결제·주문 DB 각각에서 읽기 전용 REPEATABLE READ 트랜잭션과 서버 측 커서를 사용한다. 당일 거래 외에 취소 계산에 필요한 과거 원결제와 수단 구성도 함께 추출한다. 두 DB의 전역 동일 시점 스냅샷은 아니므로 마감 확인과 참조 검사로 게시 여부를 판단한다.

각 파일의 타입·행 수·크기·SHA-256을 manifest에 기록하고, 파일 작성이 끝난 후 bundle을 확정한다. 추출 실패로 남은 `.partial` 디렉터리는 입력으로 사용하지 않는다. 완료된 bundle을 수정하지 말고 새로 추출한다.

## 3. 정산·검증·게시

```bash
"$FILE_BATCH_PYTHON" -m file_batch run \
  --bundle data/file-batch/input/date=2026-05-20/bundle=실제_UUID \
  --expected-revision 0
```

첫 게시는 revision 0을 기대한다. 정정 재계산은 현재 게시 revision을 명시한다. 같은 날짜의 다른 실행이 먼저 게시하면 버전 충돌로 실패하며 자동으로 최신 결과를 덮어쓰지 않는다.

- 입력은 실행 디렉터리로 복사한 뒤 체크섬·스키마를 검사한다. 계산 중 외부 파일을 다시 읽지 않는다.
- 입력·코드·계산 정책으로 실행 키를 만든다. DB 유일성 제약과 lease·소유 토큰으로 중복 실행 및 오래된 실행의 게시를 막는다.
- 같은 실행이 완료됐다면 기존 run_id를 반환한다. 실패한 실행은 같은 bundle로 새 attempt를 만든다.
- dbt 계산과 필수 대사가 통과해야 게시 후보를 적재한다. 후보 값은 DB에서 전수 대조한다.
- 날짜별 결과 교체·게시 revision 변경·실행 완료 상태를 하나의 PostgreSQL 트랜잭션으로 커밋한다.

실행 산출물은 `data/file-batch/runs/<run_id>/attempt=N/`에 보관한다. `input-checks.json`, `output-checks.json`, `dbt.log`, Parquet 결과, `work.duckdb`, 성공·실패 JSON을 확인할 수 있다.

## 4. 마감 확정과 재마감

```bash
"$FILE_BATCH_PYTHON" -m file_batch finalize --date 2026-05-20 --revision 1 --reason '마감 확인'
"$FILE_BATCH_PYTHON" -m file_batch reopen --date 2026-05-20 --revision 1 --reason '승인된 원장 정정'
```

확정된 날짜는 새 결과 게시가 차단된다. 재마감은 명시적 사유와 현재 revision을 요구하고 DB에 이력을 남긴다. 별도의 사용자 승인 UI·RBAC는 구현 범위에 포함하지 않으며 CLI 실행 권한으로 통제한다.

## 5. 두 경로 결과 검증

### 저장된 실제 CDC 랜딩과 비교

같은 원천 데이터가 반영된 CDC 랜딩을 준비하고 컨슈머를 정상 종료해 버퍼를 flush한다. 그 상태의 DB에서 파일 bundle을 추출한다. 검증 중에는 원천 데이터를 변경하지 않는다.

```bash
bash scripts/verify-file-batch.sh \
  --bundle data/file-batch/input/date=2026-05-20/bundle=실제_UUID \
  --landing data/landing
```

검증 명령은 다음을 수행한다. 결과를 서비스 DB에 게시하지 않는다.

1. bundle과 CDC 랜딩을 별도 디렉터리로 복사·고정한다.
2. 기존 `e2e_load.sql`로 CDC 랜딩의 `_seq` 중복 제거·삭제를 반영한다.
3. 동일 bundle의 기준정보를 사용해 기존 `03_settlement.sql`을 실행한다.
4. 파일 입력으로 새 dbt 정산을 독립 실행한다.
5. 대상 날짜와 원결제·주문의 입력 범위를 양방향 전수 비교한다.
6. `settlement_event`와 `settlement_agg`의 모든 컬럼을 타입 정규화 후 `EXCEPT ALL`로 양방향 비교한다. 행 중복도 차이로 판정한다.

차이가 있으면 종료 코드 1을 반환하고, 어느 테이블·방향에서 다른지와 샘플 행을 JSON으로 남긴다. 보고서는 `data/file-batch/verification/<UUID>/`의 `input-comparison.json`, `result-comparison.json`, `summary.json`이다. 두 DB 결과만 비교하려면 `python -m file_batch compare --legacy-db ... --file-db ... --date ... --report ...`를 사용한다.

### 실제 CDC 수집을 포함한 검증

```bash
(cd normalize && ./gradlew --gradle-user-home ../.gradle-file-batch test bootJar --no-daemon)
bash scripts/verify-file-batch-live.sh
```

전용 Compose 프로젝트 `settlement-file-cdc-test`를 사용하며 55440~55444 포트가 필요하다. 소규모 공통 원장을 PostgreSQL에 적재한 후 **실제 Debezium Avro → Kafka → 기존 Java 컨슈머**를 통과시킨다. 초기 snapshot 이후 UPDATE·INSERT·DELETE도 발생시킨다. 동일 DB를 파일로 추출해 정산·게시·재실행하고, CDC 랜딩의 정산 결과와 입력·명세·집계를 전수 비교한다.

기존 커넥터·오프셋이 남은 테스트 환경에서는 실행을 거부한다. 재실행 전에는 아래 명령으로 이 전용 테스트 프로젝트만 초기화한다. 기존 `settlement-pipeline` 서비스는 사용하지 않는다.

```bash
export POSTGRES_USER=file_batch_test POSTGRES_PASSWORD=file_batch_test_local
docker compose -p settlement-file-cdc-test \
  -f infra/file-batch/compose.yml -f infra/file-batch/compose.cdc-test.yml down -v
```

Registry 2.4.1의 동시 스키마 등록 과정에서 날짜 역직렬화 오류가 관측되어 검증 스크립트는 결제·주문 커넥터를 순차 등록한다. 이는 검증 초기화 방식이며 기존 Java 앱·커넥터 파일은 변경하지 않는다.

## 6. 자동 테스트

```bash
bash scripts/test-file-batch.sh
# 실제 DB 검증을 포함하려면 분리된 테스트 DB의 FILE_BATCH_TEST_DSN을 지정한다.
# 테스트 계정은 file_batch_test만 허용한다. 테스트 DB의 테이블을 초기화한다.
FILE_BATCH_TEST_DSN='테스트_DB_DSN' bash scripts/test-file-batch.sh
```

DB 환경변수가 없으면 PostgreSQL 통합 테스트는 skip으로 표시된다. 통합 테스트에서는 소스용 `paydb`·`orderdb`를 만들고 초기화하므로 운영·개발 공용 DB를 지정하지 않는다. JUnit 형식 결과는 `data/file-batch/test-results/pytest.xml`에 기록한다.

테스트는 입력 위변조·누락·중복, 금액·참조 검사, 반올림 경계, 취소일과 원결제일 분리, 기존 생성기 1,000주문 회귀, 두 경로 입력·결과 비교, 실행 충돌·lease 만료·게시 롤백·버전 충돌·마감 확정을 다룬다.

## 현재 업무 범위

- 기존과 동일하게 주문당 하나의 원결제, 상품 라인 단위 취소를 지원한다. 동일 주문 재결제·동일 수단의 복수 MID는 명확한 정책 없이 계산하지 않고 거부한다.
- 금액은 원 단위 정수, 입력 거래·상품 금액은 1조 원 이하로 제한한다. 배분·수수료 반올림은 HUGEINT 정수 몫으로 계산해 부동소수점 중간 오차를 피한다.
- 취소 수수료 반환 정책은 추가하지 않았다. 기존과 같이 `payout = gross - cancel - PAY 수수료`다.
- 현재 합성 생성기에는 원결제보다 취소 시각이 빠른 데이터도 있다. 기존 결과와의 호환성을 위해 사건 시각 선후관계를 새 필수 검사로 추가하지 않았다.
- 같은 날짜의 동일 상품 라인 중복 취소는 차단하지만 날짜를 넘어선 누적 취소 한도는 별도 이력 계약이 필요하다. 수량·금액 단위 반복 취소는 지원하지 않는다.
- 최근 N일 자동 재추출, 외부 마감 신호 자동 수신, 외부 알림 전송, 실제 PG 독립 대사, 지급 실행, 운영 HA는 후속 범위다. 현재는 close 파일과 CLI 재실행·종료 코드·실행 기록을 제공한다.
