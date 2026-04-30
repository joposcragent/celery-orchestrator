#!/usr/bin/env bash
# Build Docker image joposcragent/sentence-transformer:<version> and tag :latest.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

read_project_version() {
  awk '
    $0 == "[project]" { p=1; next }
    p && /^\[/ { exit }
    p && $1 ~ /^version$/ {
      line = substr($0, index($0, "=") + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
      gsub(/^"|"$/, "", line)
      print line
      exit
    }
  ' "$ROOT/pyproject.toml"
}

IMAGE_NAME="${DOCKER_IMAGE_NAME:-joposcragent/celery-orchestrator}"
IMAGE_VERSION="${DOCKER_IMAGE_VERSION:-$(read_project_version)}"
REF="${IMAGE_NAME}:${IMAGE_VERSION}"
LATEST_REF="${IMAGE_NAME}:latest"

echo "[build:image] docker build -t ${REF} ."
docker build -t "${REF}" .

echo "[build:image] docker tag ${REF} ${LATEST_REF}"
docker tag "${REF}" "${LATEST_REF}"
