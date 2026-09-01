# Airflow 오케스트레이터 — 기존 셸 스크립트를 BashOperator로 그대로 실행.
#   태스크가 docker run을 쓰므로 호스트 docker CLI가 필요 → 공식 CLI 이미지에서 정적 바이너리만 복사.
FROM docker:27.5.1-cli AS dockercli
FROM apache/airflow:2.10.5
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
