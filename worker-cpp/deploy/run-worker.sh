#!/usr/bin/env bash
set -euo pipefail

WORKER_PORT="${WORKER_PORT:-7001}"
WORKER_THREADS="${WORKER_THREADS:-2}"
DATA_DIR="${DATA_DIR:-$(pwd)/data}"

mkdir -p "${DATA_DIR}"

docker run --rm --gpus all \
  --name ae-worker \
  -p "${WORKER_PORT}:7001" \
  -e WORKER_PORT=7001 \
  -e WORKER_THREADS="${WORKER_THREADS}" \
  -e NVIDIA_VISIBLE_DEVICES=all \
  -e NVIDIA_DRIVER_CAPABILITIES=compute,video,utility \
  -v "${DATA_DIR}:/data" \
  ae-worker:latest
