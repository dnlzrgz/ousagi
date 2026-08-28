#!/usr/bin/env python3

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "scripts" / "bench.py"


def main():
    thread_counts = [int(t) for t in sys.argv[1:]] or [1, 4, 8]
    run_dir = ROOT / "bench-results" / datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    for threads in thread_counts:
        print(f"=== OUSAGI_THREADS={threads} ===", file=sys.stderr)
        log_file = run_dir / f"threads-{threads}.log"
        env = os.environ.copy()
        env["OUSAGI_THREADS"] = str(threads)
        with log_file.open("w") as f:
            subprocess.run([sys.executable, str(BENCH)], env=env, stdout=f, check=True)

    print(f"==> Done. Results saved under {run_dir}/", file=sys.stderr)


if __name__ == "__main__":
    main()
