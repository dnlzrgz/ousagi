#!/usr/bin/env python3
"""
Run a memtier_benchmark pass against ousagi or memcached.

Usage:
    scripts/bench.py
    scripts/bench.py --target memcached
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = ROOT / "docker" / "docker-compose.bench.yml"
COMPOSE = ["docker", "compose", "-f", str(COMPOSE_FILE)]

# Both services listen on 11211 inside the `bench` network.
TARGETS = {
    "ousagi": {"service": "ousagi", "server": "ousagi", "port": "11211"},
    "memcached": {"service": "memcached", "server": "memcached", "port": "11211"},
}

COMMON_ARGS = [
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

log = logging.getLogger("bench")


def run(*args, check=True):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, check=check)


def run_memtier(*extra_args):
    cmd = COMPOSE + ["run", "--rm", "-T", "memtier", *extra_args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def memtier_args(target: dict, extra: list[str]) -> list[str]:
    return [
        f"--server={target['server']}",
        f"--port={target['port']}",
        *COMMON_ARGS,
        *extra,
    ]


def warmup(target: dict) -> None:
    log.info(f"Warming up {target['service']}")
    result = run_memtier(*memtier_args(target, WARMUP_EXTRA_ARGS))
    if result.returncode != 0:
        log.error(f"Warmup failed:\n{result.stdout + result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, result.args)


def measure(target: dict) -> str:
    log.info(f"Running benchmark against {target['service']}")
    result = run_memtier(*memtier_args(target, MEASURE_EXTRA_ARGS))
    if result.returncode != 0:
        log.error(f"Benchmark failed:\n{result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, result.args)

    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=sorted(TARGETS),
        default="ousagi",
        help="server under test (default: %(default)s)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )

    target = TARGETS[args.target]

    run("up", "-d", "--build", target["service"])
    try:
        warmup(target)
        output = measure(target)
    finally:
        log.info("Tearing down containers")
        run("down", "-v", check=False)

    print(output)


if __name__ == "__main__":
    main()
