#!/usr/bin/env bash
# Fast/DB tests. Set FILE_BATCH_TEST_DSN for actual PostgreSQL integration tests.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${FILE_BATCH_PYTHON:-.venv-file-batch/bin/python}"
mkdir -p data/file-batch/test-results
exec "$PYTHON" -m pytest file_batch/tests -q --junitxml=data/file-batch/test-results/pytest.xml "$@"
