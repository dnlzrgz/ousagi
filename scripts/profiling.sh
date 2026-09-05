#!/usr/bin/env bash
#
# Usage:
#   scripts/profiling.sh                     # 5 iterations against 127.0.0.1:11211
#   scripts/profiling.sh -i 10               # 10 iterations
#   scripts/profiling.sh -h 127.0.0.1 -p 11212
#   scripts/profiling.sh -s                  # skip warmup
#
set -euo pipefail

HOST="127.0.0.1"
PORT="11211"
ITERATIONS="5"
LABEL="native"
SKIP_WARMUP="0"
STARTUP_TIMEOUT="30"

usage() {
  echo "Usage: $0 [-h host] [-p port] [-i iterations] [-l label] [-s] [-w startup_timeout]" >&2
  exit 1
}

while getopts ":h:p:i:l:w:s" opt; do
  case "$opt" in
    h) HOST="$OPTARG" ;;
    p) PORT="$OPTARG" ;;
    i) ITERATIONS="$OPTARG" ;;
    l) LABEL="$OPTARG" ;;
    w) STARTUP_TIMEOUT="$OPTARG" ;;
    s) SKIP_WARMUP="1" ;;
    *) usage ;;
  esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MEMTIER_IMAGE="redislabs/memtier_benchmark:2.5.1"

MEMTIER_COMMON=(
  "--server=$HOST" "--port=$PORT"
  --protocol=memcache_text
  --clients=50 --threads=4
  --data-size=128
  --key-minimum=1 --key-maximum=100000
)
WARMUP_ARGS=(--ratio=1:0 --key-pattern=P:P)
MEASURE_ARGS=(--ratio=1:10 --key-pattern=R:R --test-time=30 --print-percentiles=50,90,99,99.9)

run_memtier() {
  docker run --rm -i --network host "$MEMTIER_IMAGE" "$@"
}

echo "Waiting for $HOST:$PORT to accept connections (timeout ${STARTUP_TIMEOUT}s)..." >&2
deadline=$((SECONDS + STARTUP_TIMEOUT))
until (exec 3<>"/dev/tcp/$HOST/$PORT") 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "error: timed out waiting for $HOST:$PORT. Is the server actually running?" >&2
    exit 1
  fi
  sleep 0.5
done
exec 3<&- 3>&-
echo "Server is up." >&2

if [[ "$SKIP_WARMUP" == "1" ]]; then
  echo "Skipping warmup (-s)" >&2
else
  echo "Warming up keyspace..." >&2
  run_memtier "${MEMTIER_COMMON[@]}" "${WARMUP_ARGS[@]}" >/dev/null
fi

RUN_DIR="$ROOT/memtier_benchmark/$(date +%Y%m%d%H%M%S)"
mkdir -p "$RUN_DIR"

for ((i = 1; i <= ITERATIONS; i++)); do
  echo "Measurement pass $i/$ITERATIONS..." >&2
  run_memtier "${MEMTIER_COMMON[@]}" "${MEASURE_ARGS[@]}" > "$RUN_DIR/${LABEL}-iter-${i}.log"
done

echo "Done. $ITERATIONS result(s) saved under $RUN_DIR/" >&2
