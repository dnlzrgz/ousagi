#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

THREAD_COUNTS=("${@:-}")
if [ "$#" -eq 0 ]; then
  THREAD_COUNTS=(1 4)
else
  THREAD_COUNTS=("$@")
fi

RUN_ID="$(date +%Y%m%d%H%M%S)"
RUN_DIR="bench-results/${RUN_ID}"
mkdir -p "$RUN_DIR"

for threads in "${THREAD_COUNTS[@]}"; do
  echo "=== OUSAGI_THREADS=${threads} ==="
  OUSAGI_THREADS="$threads" ./scripts/bench.sh 2>&1 | tee "${RUN_DIR}/threads-${threads}.log"
done

echo "==> Done. Results saved under ${RUN_DIR}/"
