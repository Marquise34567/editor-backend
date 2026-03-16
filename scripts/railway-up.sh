#!/usr/bin/env bash
set -euo pipefail

# Work around buildx progress panics by forcing plain progress output.
export BUILDKIT_PROGRESS=plain
export DOCKER_BUILDKIT=1

railway "$@"
