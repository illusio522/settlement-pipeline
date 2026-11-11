# 파일 배치 검증 결과

검증일: 2026-10-18. 로컬 합성 데이터로 검증했으며 운영 데이터·부하 검증 결과는 아니다.

기존 구현은 `archive/cdc-lakehouse` (`2dd8d14`), 파일 경로는 `feat/file-batch`에 있다. 비교 기준인 기존 `transform/01_schema.sql`, `e2e_load.sql`, `03_settlement.sql`과 Java 앱 소스는 변경하지 않았다.

## 자동 테스트

| 구분 | 결과 | 확인한 내용 |
|---|---|---|
| Python 전체 | 37개 통과, skip 없음 | 입력 파일 검증, 금액·참조 검사, 두 계산 경로 비교, PostgreSQL 추출·게시, 실행 키 안정성 |
| 기존 Java normalize | 13개 통과 | CDC 파싱 |
| 기존 Java serving | 8개 통과 | API·수단 매핑 |
| Docker | 이미지 빌드·CLI 실행 성공 | 고정 의존성의 Linux 설치 및 실행 |
| Docker 내 계산 테스트 | 4개 통과 | 세 날짜 결과 동등성 및 반올림 경계 |

Python 테스트는 실제 PostgreSQL을 연결해 실행했다. 동시 실행, lease 만료 후 이전 소유자 차단, 실패 재시도, 날짜별 교체, 게시 중 오류 롤백, 게시 버전 충돌, 마감 확정·재마감을 포함한다.

기존 생성기 **1,000주문**을 사용한 회귀 테스트에서는 세 날짜의 기존 SQL과 파일 모델 결과를 비교했다. 이 회귀 테스트의 CDC 입력은 테스트에서 구성한 NDJSON이며, 실제 Kafka 수집 검증은 아래 별도 실행이다.

## 실제 CDC와 파일 경로 비교

분리된 Docker 환경에서 PostgreSQL → Debezium Avro → Kafka → 기존 Java 컨슈머로 수집했다. 초기 snapshot 이후 UPDATE·INSERT·DELETE를 발생시키고, 동일 DB에서 Parquet 입력을 추출했다.

| 확인 항목 | 결과 |
|---|---|
| CDC 랜딩 | 초기 13건과 후속 변경 5건, 총 18건 |
| 입력 비교 | 대상 거래·원결제·주문·기준정보 등 9개 테이블 차이 0건 |
| `settlement_event` | 양쪽 6행, 전체 15컬럼 양방향 차이 0건 |
| `settlement_agg` | 양쪽 5행, 전체 10컬럼 양방향 차이 0건 |
| 파일 결과 게시 | revision 1, `PUBLISHED` |
| 동일 bundle 재실행 | 기존 run_id 반환, 중복 게시 없음 |

비교는 행 수·합계만 확인하지 않고 모든 컬럼을 `EXCEPT ALL`로 대조한다. 따라서 값 차이와 중복 행도 실패로 처리한다. 건수가 같아도 값이 다른 경우, 결과가 같아도 입력 식별자가 다른 경우를 별도 테스트로 검증했다.

실행 증거는 로컬의 다음 경로에 남아 있다. `data/` 산출물은 Git에 포함하지 않으며 아래 명령으로 새 보고서를 생성할 수 있다.

```text
data/file-batch/test-results/pytest.xml
data/file-batch/live-verification/8cf503c4-6cf1-40e2-8402-ae6cec94aff1/acceptance.json
```

`acceptance.json`에서 입력·결과 비교 보고서와 양쪽 DuckDB 산출물 경로를 확인할 수 있다. 이번 입력 manifest의 SHA-256은 `1fb880b275ca81d239689f56fc375371fe813a622cc3d2ed303ee35c9a255b2b`다.

## 재현

```bash
# 테스트 전용 DB 접속 정보는 FILE_BATCH_TEST_DSN으로 주입
bash scripts/test-file-batch.sh

# 실제 CDC 수집을 포함한 비교: Docker와 빌드된 Java 컨슈머 필요
bash scripts/verify-file-batch-live.sh

# 이미 확보한 동일 원천의 입력 비교
bash scripts/verify-file-batch.sh --bundle <bundle> --landing <landing>
```

DB 환경변수가 없으면 통합 테스트가 skip된다. 테스트 DB 준비와 CDC 스택 초기화 조건은 [실행 가이드](FILE-BATCH.md)를 따른다.

현재 동일성 검증은 주문당 하나의 원결제·라인 단위 취소라는 지원 범위에 대한 것이다. 날짜를 넘는 누적 취소 한도, 자동 지연 데이터 재추출, 실제 PG 독립 대사·지급, 운영 고가용성은 검증 완료 범위가 아니다.
