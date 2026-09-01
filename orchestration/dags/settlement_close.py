# 일 정산 마감 DAG — 검증된 셸 스크립트를 의존성 그래프로 연결.
#   랜딩(NDJSON) → MinIO Parquet → Iceberg 테이블 → 정산 재구성 + 대사(checks)
#   ★ bash_command 끝 공백: .sh로 끝나면 Airflow가 Jinja 템플릿 파일로 오인(TemplateNotFound) → 공백으로 회피.
#   스크립트가 자체적으로 `cd 저장소루트` + `docker run -v "$PWD":/work` 를 수행하므로
#   저장소가 호스트와 동일 절대경로(REPO_HOST_PATH)로 마운트돼 있으면 무수정 동작한다.
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = os.environ["REPO_HOST_PATH"]

default_args = {
    "retries": 1,                          # 일시 오류(컨테이너 기동 경합 등) 1회 재시도
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="settlement_close",
    description="일 정산 마감: 랜딩→레이크(Parquet)→Iceberg→정산·대사",
    schedule="@daily",                     # 정의만 — 생성 시 paused, 로컬은 수동 트리거
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["settlement"],
) as dag:
    # ① 로컬 랜딩 NDJSON → MinIO Parquet (_op·_seq 보존)
    landing_to_minio = BashOperator(
        task_id="landing_to_minio",
        bash_command=f"cd {REPO} && bash scripts/landing-to-minio.sh ",
    )

    # ② 레이크 Parquet → Iceberg 테이블 (쓰기 시점 dedup 적용)
    build_iceberg = BashOperator(
        task_id="build_iceberg",
        bash_command=f"cd {REPO} && bash scripts/build-iceberg.sh ",
    )

    # ③ Iceberg → canonical 재구성 → 정산 배치 → 돈보존·MID 대사 (diff=0 검증 포함)
    settle_and_check = BashOperator(
        task_id="settle_and_check",
        bash_command=f"cd {REPO} && bash scripts/run-e2e-iceberg.sh ",
    )

    landing_to_minio >> build_iceberg >> settle_and_check
