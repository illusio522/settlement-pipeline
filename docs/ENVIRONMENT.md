# 로컬 환경 검토

검토일 기준 이 머신 상태와 셋업 가이드.

## 현재 환경

| 항목 | 상태 | 비고 |
|---|---|---|
| Docker | 28.3.3 + Compose v2.39.2 ✅ | 데몬 실행중 |
| Java | 21.0.10 LTS ✅ | (인프라는 도커라 직접 필요X) |
| Python | 3.14.5 ⚠️ | **너무 최신** — 아래 주의 |
| git / brew | ✅ | |
| CPU / RAM / Disk | **10코어 / 64GB / 613GB 여유** ✅✅ | 풀스택 쾌적 |

→ 결론: 린 풀스택(Postgres·Debezium·Kafka(KRaft)·MinIO·DuckDB) 로컬 구동에 충분.

## ⚠️ 주의: Python 3.14

dbt·duckdb 등 일부 파이썬 패키지가 3.14 휠을 아직 안 낼 수 있음. 두 가지 우회:

**옵션 A (권장): dbt/DuckDB를 도커로**
- 파이썬 버전 의존 제거, 재현성↑

**옵션 B: pyenv로 3.12 가상환경**
```bash
brew install pyenv
pyenv install 3.12.8
pyenv local 3.12.8
python -m venv .venv && source .venv/bin/activate
pip install duckdb dbt-duckdb
```

## 도커 리소스 권장
- Docker Desktop → Settings → Resources
- 메모리 8GB+ 할당 (64GB 머신이라 여유)
- 풀스택 컨테이너: postgres, debezium(connect), kafka(KRaft), minio (+ console UI들)

## 포트 계획 (충돌 점검 필요)
| 서비스 | 포트 |
|---|---|
| Postgres | 5432 |
| Kafka (KRaft, 브로커) | 9092 |
| Kafka UI | 8080 |
| Debezium Connect | 8083 |
| MinIO (S3) | 9000 |
| MinIO Console | 9001 |

> 기존 프로젝트(payment 등)와 포트가 겹치면 compose에서 변경.

## 다음 액션
1. `infra/docker-compose.yml` 작성 (postgres + kafka(KRaft) + minio + debezium)
2. `infra/postgres/init.sql` (소스 스키마 + Outbox + WAL 설정)
3. `generator/` 결제 데이터 생성기 (스큐 주입)
4. 1단계 MVP: 생성기 → DuckDB 정산 집계 + 대사
