#!/usr/bin/env python3

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = [
    "docker",
    "compose",
    "-f",
    "docker/docker-compose.bench.yml",
]


def run(*args, check=True):
    subprocess.run(COMPOSE + list(args), cwd=ROOT, check=check)


def main():
    run("up", "-d", "--build", "ousagi")
    try:
        subprocess.run(
            COMPOSE + ["run", "--rm", "-T", "memtier"],
            cwd=ROOT,
            check=True,
        )
    finally:
        run("down", "-v", check=False)


if __name__ == "__main__":
    main()
