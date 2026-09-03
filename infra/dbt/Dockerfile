# dbt-duckdb 실행용 (호스트 파이썬 3.14 회피 → 컨테이너 파이썬 3.11)
FROM python:3.11-slim
RUN pip install --no-cache-dir "dbt-duckdb==1.9.6"
WORKDIR /work/dbt
ENTRYPOINT ["dbt"]
