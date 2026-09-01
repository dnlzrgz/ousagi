#!/usr/bin/env python3
"""
Run a memtier_benchmark pass against ousagi or memcached with a different thread count
and saves the results.

Usage:
    scripts/threads.py
    scripts/threads.py 1 2 4 8
    scripts/threads.py --target memcached 4 8
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "scripts" / "bench.py"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "threads",
        nargs="*",
        type=int,
        default=[1, 4],
        help="thread counts to sweep (default: 1 4)",
    )
    parser.add_argument(
        "--target",
        choices=["ousagi", "memcached"],
        default="ousagi",
        help="server under test (default: %(default)s)",
    )
    args = parser.parse_args()

    run_dir = ROOT / "memtier_benchmark" / datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    for threads in args.threads:
        print(f"=== target={args.target} threads={threads} ===", file=sys.stderr)
        log_file = run_dir / f"{args.target}-threads-{threads}.log"

        with log_file.open("w") as f:
            subprocess.run(
                [sys.executable, str(BENCH), "--target", args.target],
                stdout=f,
                check=True,
            )

    print(f"results saved under {run_dir}/", file=sys.stderr)


if __name__ == "__main__":
    main()
