# settlement-pipeline

파일 기반 일 마감 정산 파이프라인. 결제·주문 데이터를 Parquet로 고정하고, DuckDB·dbt로 복합결제·부분취소 정산을 계산한 뒤 정합성 검사를 통과한 결과만 PostgreSQL에 게시한다.

```text
PostgreSQL 결제·주문
  → Parquet + manifest
  → DuckDB / dbt
  → 입력·금액 정합성 검사
  → 날짜별 원자 게시
  → Spring Boot 조회 API
```

## 브랜치

| 브랜치 | 용도 |
|---|---|
| `main` | 현재 기준 브랜치. 파일 배치 기본 경로와 기존 CDC 코드를 함께 담는다 |
| `archive/cdc-lakehouse` | 기존 CDC·Kafka·레이크하우스 구현과 당시 문서 보존 |
| `feat/file-batch` | 파일 배치 기본 경로, 멱등 실행·검증 게이트·두 경로 동등성 검증. `main`에 머지했다 |

기존 `normalize/`, `transform/`, `dbt/`, CDC 인프라는 확장 및 회귀 비교용으로 유지했다. 파일 배치 실행에는 Kafka·Debezium·MinIO·Spark가 필요하지 않다. 실제 CDC와의 수집 구간 비교를 실행할 때만 별도 검증 스택을 사용한다.

## 문서와 진입점

- [파일 배치 실행·복구·동등성 검증](docs/FILE-BATCH.md)
- [실행한 테스트와 결과 비교 기록](docs/FILE-BATCH-VERIFICATION.md)
- [성능 측정과 CDC 비교](docs/FILE-BATCH-BENCHMARK.md)
- [설계 기준](docs/FILE-BATCH-DESIGN.md)
- [기존 CDC 실행 가이드](docs/RUNBOOK.md)
- [기존 성능 실험](docs/BENCHMARK.md)

| 경로 | 역할 |
|---|---|
| `file_batch/` | 추출, 불변 bundle, dbt 정산, 실행 소유권, 게시·비교 CLI |
| `file_batch/tests/` | 공통 데이터 비교, 입력 검사, 실제 PostgreSQL 통합 테스트 |
| `infra/file-batch/compose.yml` | 분리된 최소 PostgreSQL·배치 컨테이너 |
| `scripts/test-file-batch.sh` | 자동 테스트·JUnit 결과 생성 |
| `scripts/verify-file-batch.sh` | 기존 CDC 랜딩과 파일 bundle의 입력·명세·집계 전수 비교 |
| `scripts/verify-file-batch-live.sh` | 실제 PostgreSQL WAL→Debezium→Kafka→Java 컨슈머 경유 비교 |
| `bench/file-batch-bench.sh` | 파일 배치 규모별 단계 소요 시간 측정 |
| `bench/cdc-bench.sh` | 같은 데이터·규모로 CDC 경로 단계 소요 시간 측정 |
| `serving/` | 기존 일자별 정산 요약·파트너 상세 조회 API |

## 기본 검증

```bash
python3.11 -m venv .venv-file-batch
.venv-file-batch/bin/pip install -r file_batch/requirements.lock
bash scripts/test-file-batch.sh
```

PostgreSQL 통합 테스트는 별도 테스트 DB와 `FILE_BATCH_TEST_DSN`이 필요하다. 실제 CDC 비교는 Docker·Java 21이 필요하며 자세한 절차는 실행 가이드에 있다. 상류 마감 명세와 기준정보는 명시적 입력 계약으로 받는다. 실제 지급 실행과 운영 HA는 범위 밖이다.

## 성능

같은 머신·데이터 조건의 100만 주문에서 마감 기준 전체 소요는 파일 배치 18.07초, CDC 189.89초다. 파일 배치는 1,000만 주문(하루 거래 550만 건)을 158초에 처리했다. 측정 조건, 단계별 시간, CDC가 느린 원인은 [성능 측정과 CDC 비교](docs/FILE-BATCH-BENCHMARK.md)에 정리했다.
