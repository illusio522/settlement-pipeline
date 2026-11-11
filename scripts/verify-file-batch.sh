#!/usr/bin/env bash
# 종료된 실제 CDC 랜딩과 동일 원천에서 추출한 bundle을 비교한다. DB 게시 없음.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${FILE_BATCH_PYTHON:-.venv-file-batch/bin/python}"
exec "$PYTHON" -m file_batch verify "$@"
