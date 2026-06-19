# settlement-pipeline

대규모 파트너 정산 파이프라인 — 로컬 학습/검증용 구현.

브랜드별(무신사·29cm) 결제 데이터를 **CDC → 로그 → 레이크 → 증분 배치 → 정산/대사 → 마감**으로
처리하는 파이프라인을, 운영 설계는 확장 가능하게 / 로컬 테스트는 작게 검증한다.

## 4대 목표
고성능 · 저비용 · 빠른 처리속도 · 유지보수
핵심 레버: `증분 · 컬럼나 · 멱등 · 버스트컴퓨트 · 선언적/표준`

## 스택 (린 — right-sized)
| 역할 | 기술 |
|---|---|
| 소스 DB (+Outbox) | PostgreSQL |
| CDC | Debezium |
| 로그/버퍼 | Redpanda (Kafka 호환) |
| 오브젝트스토리지 | MinIO (S3 호환) |
| 레이크 포맷 | Iceberg / Parquet |
| 집계 | dbt + DuckDB (증분) |
| 대사 | 컨트롤 토탈 스크립트 |
| 오케스트레이션 | (후순위) cron → Airflow |

> Spark·대형 Kafka는 이 규모엔 과설계 → 도입 안 함. 확장 시 엔진만 교체.

## 디렉터리

| 경로 | 용도 |
|---|---|
| `docs/` | 설계서·환경노트 |
| `infra/` | Docker 인프라 (postgres·debezium·redpanda·minio) |
| `generator/` | 결제 데이터 생성기 (스큐 주입) |
| `normalize/` | 정규화 (canonical 스키마 검증) |
| `lake/` | Iceberg/Parquet 랜딩 |
| `transform/dbt/` | 증분 멱등 집계 (dbt) |
| `reconcile/` | 3자 컨트롤 토탈 대사 |
| `orchestration/` | 스케줄/DAG |
| `bench/` | 성능 측정 훅·부하 테스트 |
| `scripts/` | 헬퍼 |

## 빌드 단계 (점진)
- **1단계** Postgres + 생성기 → DuckDB로 정산 집계+대사 (CDC·로그 없이 본질부터)
- **2단계** Debezium + Redpanda → 실시간 수집 연결
- **3단계** MinIO + Iceberg + dbt 증분배치 → 레이크 완성
- **4단계** 스키마진화·스큐·exactly-once·성능측정 데모

## 검증 시나리오
멱등(두 번 돌려도 동일) · 정확히 한 번 · 대사 불일치 탐지 · 스키마 진화 · 핫파트너 스큐

## 테스트 파라미터 (조정 가능)
브랜드 2 / 파트너 100(무신사 70% 스큐) / 결제 100만~1,000만(1억 옵션) / 일 단위 정산

자세한 설계는 [docs/DESIGN.md](docs/DESIGN.md), 환경은 [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).
