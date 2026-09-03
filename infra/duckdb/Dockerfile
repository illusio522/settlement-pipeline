# DuckDB CLI 컨테이너 — 정산 컴퓨트(1단계: 조인·배분·freeze·집계·대사)
FROM debian:bookworm-slim
ARG DUCKDB_VERSION=1.5.3
ARG TARGETARCH
# 릴리스 zip은 sha256 검증 후에만 설치 (공급망 변조·전송 오류 차단)
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
 && curl -fsSL -o /tmp/d.zip "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${TARGETARCH}.zip" \
 && case "${TARGETARCH}" in \
      arm64) echo "5e2399428793642e994f1584c47d49f4c58b7b4ec2297ea4a522353a6c553835  /tmp/d.zip" | sha256sum -c - ;; \
      amd64) echo "35caef1fecbc8d7e2c07de4fd2cdefc5189ec9ba9e1cca228fb1a1c48cc52a8a  /tmp/d.zip" | sha256sum -c - ;; \
      *)     echo "미지원 아키텍처: ${TARGETARCH} (체크섬 미등록)"; exit 1 ;; \
    esac \
 && unzip /tmp/d.zip -d /usr/local/bin \
 && rm /tmp/d.zip \
 && apt-get purge -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
WORKDIR /work
ENTRYPOINT ["duckdb"]
