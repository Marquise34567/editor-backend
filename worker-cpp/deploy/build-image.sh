#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

docker build -t ae-worker:latest -f "${WORKER_ROOT}/Dockerfile" "${WORKER_ROOT}"
