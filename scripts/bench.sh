#!/usr/bin/env bash
#
# Run a memtier_benchmark pass against ousagi (default) or memcached.
#
# Usage:
#   scripts/bench.sh                    # ousagi, 4 threads
#   scripts/bench.sh -t memcached       # memcached, 4 threads
#   scripts/bench.sh -n 1               # ousagi, 1 thread
#   scripts/bench.sh -t memcached -n 2  # memcached, 2 threads
#
set -euo pipefail

TARGET="ousagi"
THREADS="4"

usage() {
  echo "Usage: $0 [-t ousagi|memcached] [-n threads(1-4)]" >&2
  exit 1
}

while getopts ":t:n:h" opt; do
  case "$opt" in
    t) TARGET="$OPTARG" ;;
    n) THREADS="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

if [[ "$TARGET" != "ousagi" && "$TARGET" != "memcached" ]]; then
  echo "error: -t must be 'ousagi' or 'memcached' (got '$TARGET')" >&2
  usage
fi

if ! [[ "$THREADS" =~ ^[1-4]$ ]]; then
  echo "error: -n must be an integer from 1 to 4 (got '$THREADS')" >&2
  usage
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.bench.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")

MEMTIER_COMMON=(
  "--server=$TARGET" --port=11211
  --protocol=memcache_text
  --clients=50 --threads=4
  --data-size=128
  --key-minimum=1 --key-maximum=100000
)
WARMUP_ARGS=(--ratio=1:0 --key-pattern=P:P)
MEASURE_ARGS=(--ratio=1:10 --key-pattern=R:R --test-time=30 --print-percentiles=50,90,99,99.9)

cleanup() {
  echo "Tearing down containers..." >&2
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Starting $TARGET with $THREADS thread(s)..." >&2
THREADS="$THREADS" "${COMPOSE[@]}" up -d --build "$TARGET"

echo "Warming up keyspace..." >&2
"${COMPOSE[@]}" run --rm -T memtier "${MEMTIER_COMMON[@]}" "${WARMUP_ARGS[@]}" >/dev/null

echo "Running benchmark..." >&2
"${COMPOSE[@]}" run --rm -T memtier "${MEMTIER_COMMON[@]}" "${MEASURE_ARGS[@]}"
