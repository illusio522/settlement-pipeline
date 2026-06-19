# DuckDB CLI 컨테이너 — 정산 컴퓨트(1단계: 조인·배분·freeze·집계·대사)
FROM debian:bookworm-slim
ARG DUCKDB_VERSION=1.5.3
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
 && curl -fsSL -o /tmp/d.zip "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${TARGETARCH}.zip" \
 && unzip /tmp/d.zip -d /usr/local/bin \
 && rm /tmp/d.zip \
 && apt-get purge -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
WORKDIR /work
ENTRYPOINT ["duckdb"]
