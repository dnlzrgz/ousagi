#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = [
    "docker",
    "compose",
    "-f",
    "docker/docker-compose.bench.yml",
]
WARMUP_ARGS = [
    "--server=ousagi",
    "--port=11211",
    "--protocol=memcache_text",
    "--clients=50",
    "--threads=4",
    "--data-size=128",
    "--ratio=1:0",
    "--key-pattern=P:P",
    "--key-minimum=1",
    "--key-maximum=100000",
]


def run(*args, check=True):
    subprocess.run(COMPOSE + list(args), cwd=ROOT, check=check)


def run_memtier(*extra_args):
    cmd = COMPOSE + ["run", "--rm", "-T", "memtier", *extra_args]
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def main():
    run("up", "-d", "--build", "ousagi")
    try:
        print("==> Warming up keyspace", file=sys.stderr)
        warmup = run_memtier(*WARMUP_ARGS)
        if warmup.returncode != 0:
            print(warmup.stdout, file=sys.stderr)
            print(warmup.stderr, file=sys.stderr)
            raise subprocess.CalledProcessError(warmup.returncode, warmup.args)

        print("==> Measuring", file=sys.stderr)
        result = run_memtier()
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            raise subprocess.CalledProcessError(result.returncode, result.args)
    finally:
        run("down", "-v", check=False)


if __name__ == "__main__":
    main()
