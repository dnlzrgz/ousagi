#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE_FILE="docker/docker-compose.bench.yml"

echo "==> Building images"
docker compose -f "$COMPOSE_FILE" build

echo "==> Running benchmark"
docker compose -f "$COMPOSE_FILE" up \
  --abort-on-container-exit \
  --exit-code-from memtier
STATUS=$?

echo "==> Tearing down"
docker compose -f "$COMPOSE_FILE" down -v

exit "$STATUS"
