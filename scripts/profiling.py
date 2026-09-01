#!/usr/bin/env python3
"""
Run memtier_benchmark against a running ousagi server for profiling.

Usage:
    scripts/profiling.py
"""

import argparse
import logging
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MEMTIER_IMAGE = "redislabs/memtier_benchmark:2.5.1"

COMMON_MEMTIER_ARGS = [
    "--protocol=memcache_text",
    "--clients=50",
    "--threads=4",
    "--data-size=128",
    "--key-minimum=1",
    "--key-maximum=100000",
]

WARMUP_EXTRA_ARGS = [
    "--ratio=1:0",
    "--key-pattern=P:P",
]

MEASURE_EXTRA_ARGS = [
    "--ratio=1:10",
    "--key-pattern=R:R",
    "--test-time=30",
    "--print-percentiles=50,90,99,99.9",
]

log = logging.getLogger("native_bench")


def wait_for_server(host: str, port: int, timeout: float) -> None:
    log.info("waiting for %s:%d to accept connections...", host, port)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                log.info("server is up!")
                return
        except OSError:
            time.sleep(0.5)

    raise SystemExit(
        f"timed out after {timeout:.0f}s waiting for {host}:{port}. "
        "is the server actually running?"
    )


def memtier_args(host: str, port: int, extra: list[str]) -> list[str]:
    return [
        f"--server={host}",
        f"--port={port}",
        *COMMON_MEMTIER_ARGS,
        *extra,
    ]


def run_memtier(args: list[str]) -> subprocess.CompletedProcess:
    cmd = ["docker", "run", "--rm", "-i", "--network", "host", MEMTIER_IMAGE, *args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def warmup(host: str, port: int) -> None:
    log.info("Warming up keyspace")
    result = run_memtier(memtier_args(host, port, WARMUP_EXTRA_ARGS))
    if result.returncode != 0:
        log.error("Warmup failed:\n%s", result.stdout + result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)


def measure(host: str, port: int) -> str:
    result = run_memtier(memtier_args(host, port, MEASURE_EXTRA_ARGS))
    if result.returncode != 0:
        log.error("Measurement failed:\n%s", result.stderr)
        raise subprocess.CalledProcessError(result.returncode, result.args)
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11211)
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="number of back-to-back passes (default: %(default)s)",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=30,
        help="seconds to wait for the server to start accepting connections (default: %(default)s)",
    )
    parser.add_argument(
        "--skip-warmup",
        action="store_true",
        help="skip the initial keyspace warmup",
    )
    parser.add_argument(
        "--label",
        default="native",
        help="prefix used for the saved log filenames (default: %(default)s)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    wait_for_server(args.host, args.port, args.startup_timeout)

    if args.skip_warmup:
        log.info("Skipping warmup (--skip-warmup)")
    else:
        warmup(args.host, args.port)

    run_dir = ROOT / "memtier_benchmark" / datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    for i in range(1, args.iterations + 1):
        log.info("Measurement pass %d/%d", i, args.iterations)
        output = measure(args.host, args.port)
        (run_dir / f"{args.label}-iter-{i}.log").write_text(output)

    log.info("done. %d result(s) saved under %s/", args.iterations, run_dir)


if __name__ == "__main__":
    main()
