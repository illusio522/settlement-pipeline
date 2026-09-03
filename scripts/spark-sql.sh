#!/usr/bin/env bash
# Spark SQL (burst compute, docker run --rm) — Nessie Iceberg REST 카탈로그 + MinIO.
#   DuckDB/dbt와 같은 패턴: 상시 서비스 아니라 필요할 때 컨테이너 띄워 실행.
#   S3(MinIO) 자격증명은 아래 --conf 로 Spark에 직접 주입(Nessie vend 미사용).
#   사용: bash scripts/spark-sql.sh <sql파일>   (없으면 스모크)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f infra/.env ] || { echo "infra/.env 없음 — infra/.env.example 를 복사해 만드세요"; exit 1; }
set -a; . infra/.env; set +a
NET=settlement-pipeline_default
SPARK_IMG=apache/spark:3.5.3
ICEBERG_VER=1.7.1
SQL_FILE="${1:-transform/spark_smoke.sql}"

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VER},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VER}"

docker run --rm -i --network "$NET" --user root \
  -v "$PWD":/work \
  -v settlement-spark-ivy:/root/.ivy2 \
  "$SPARK_IMG" /opt/spark/bin/spark-sql \
    --packages "$PKGS" \
    --conf spark.jars.ivy=/root/.ivy2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.type=rest \
    --conf spark.sql.catalog.nessie.uri=http://sp-nessie:19120/iceberg/main/ \
    --conf spark.sql.defaultCatalog=nessie \
    --conf spark.sql.catalog.nessie.s3.endpoint=http://sp-minio:9000 \
    --conf spark.sql.catalog.nessie.s3.path-style-access=true \
    --conf spark.sql.catalog.nessie.s3.access-key-id="$MINIO_ROOT_USER" \
    --conf spark.sql.catalog.nessie.s3.secret-access-key="$MINIO_ROOT_PASSWORD" \
    -f "/work/$SQL_FILE"
