#!/usr/bin/env bash
# Real CDC transport comparison, isolated stack. Build Java first; requires Docker Compose v2.24.4+.
set -euo pipefail
cd "$(dirname "$0")/.."
export POSTGRES_USER=file_batch_test POSTGRES_PASSWORD=file_batch_test_local
COMPOSE=(docker compose -p settlement-file-cdc-test -f infra/file-batch/compose.yml -f infra/file-batch/compose.cdc-test.yml)
"${COMPOSE[@]}" up -d --wait postgres kafka apicurio connect
PYTHON="${FILE_BATCH_PYTHON:-.venv-file-batch/bin/python}"
"$PYTHON" -m file_batch.tests.live_equivalence
# Containers/data retained to inspect failures. See FILE-BATCH.md for explicit cleanup commands.
