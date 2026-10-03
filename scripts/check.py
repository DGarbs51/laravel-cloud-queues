#!/usr/bin/env python3
"""Run the CI gates locally in parallel, then the combined 100% coverage gate.

    uv run --locked scripts/check.py

Lint, actionlint, the three type checkers, the docs build, packaging, conformance and the
test suite on every CI Python version. Prints one line per gate, and full output only for
failures. Tests use moto for SQS and local Valkey (redis://127.0.0.1:6379/15) for Redis,
and skip what is unavailable (shown in each gate's summary). CI's LocalStack, real Redis
and TLS Valkey runs, and the conformance TLS probe, stay in CI.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHONS = ("3.10", "3.11", "3.12", "3.13", "3.14")
RUN = ("uv", "run", "-q", "--locked", "--no-sync")
# Each version gets a throwaway env like a CI matrix job; uv's cache keeps this fast.
TEST = ("uv", "run", "-q", "--locked", "--isolated")
# Code roots only, like CI: ruff also formats fenced Python in Markdown.
LINTED = ("src", "tests", ".github/scripts", "scripts")
# Every conformance feature except the TLS probe, which needs CI's TLS-only Valkey.
CATALOG = json.loads((ROOT / "docs/contract/catalog.json").read_text())
FEATURES = tuple(f["id"] for f in CATALOG["features"] if f["id"] != "redis.tls")

COLOR = sys.stdout.isatty() and "NO_COLOR" not in os.environ


def paint(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if COLOR else text


def run(cmd: tuple[str, ...]) -> tuple[bool, str]:
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
    return proc.returncode == 0, proc.stdout + proc.stderr


def in_temp(build: Callable[[str], tuple[str, ...]]) -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as out:
        return run(build(out))


def packaging() -> tuple[bool, str]:
    """Run the artifact tests, then ``twine check`` the wheel and sdist, like CI."""
    ok, output = run((*RUN, "pytest", "-q", "-p", "no:cacheprovider", "-m", "packaging"))
    if not ok:
        return ok, output
    with tempfile.TemporaryDirectory() as out:
        built, build_output = run(("uv", "build", "-q", "--out-dir", out))
        if not built:
            return built, build_output
        dist = [str(path) for pattern in ("*.whl", "*.tar.gz") for path in Path(out).glob(pattern)]
        checked, check_output = run((*RUN, "twine", "check", "--strict", *dist))
    return checked, check_output if not checked else output


GATES: dict[str, Callable[[], tuple[bool, str]]] = {
    "ruff check": partial(run, (*RUN, "ruff", "check", *LINTED)),
    "ruff format": partial(run, (*RUN, "ruff", "format", "--check", *LINTED)),
    "actionlint": partial(run, ("uvx", "-q", "--from", "actionlint-py", "actionlint")),
    "ty": partial(run, (*RUN, "ty", "check")),
    "mypy": partial(run, (*RUN, "mypy")),
    "pyright": partial(run, (*RUN, "pyright")),
    "docs": partial(
        in_temp,
        lambda out: (
            *TEST,
            "--no-dev",
            "--group",
            "docs",
            "--all-extras",
            "sphinx-build",
            "-W",
            "--keep-going",
            "-q",
            "-b",
            "html",
            "docs",
            out,
        ),
    ),
    "packaging": packaging,
    "conformance": partial(
        in_temp,
        lambda out: (
            *RUN,
            "python",
            "-m",
            "tests.conformance",
            "--sqs",
            "moto",
            "--report",
            f"{out}/compatibility-report.json",
            "--only",
            *FEATURES,
        ),
    ),
    **{
        f"py{v}": partial(
            run,
            (
                *TEST,
                "--python",
                v,
                "coverage",
                "run",
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                "-m",
                "not packaging",
            ),
        )
        for v in PYTHONS
    },
}


def timed(gate: Callable[[], tuple[bool, str]]) -> tuple[bool, str, float]:
    start = time.monotonic()
    ok, output = gate()
    return ok, output, time.monotonic() - start


def summary(output: str) -> str:
    """Get the last non-empty line, minus colors, pytest's '=' padding and timing."""
    plain = re.sub(r"\x1b\[[\d;]*m|\x1b\(B", "", output)
    lines = [line for line in plain.splitlines() if line.strip()]
    return re.sub(r" in [\d.]+s.*$", "", lines[-1].strip("= ")) if lines else ""


def report(name: str, ok: bool, output: str, seconds: float, detail: str | None = None) -> None:
    status = paint("ok  ", "32") if ok else paint("FAIL", "31;1")
    line = f"{status} {name:<12} {seconds:6.1f}s  {paint(detail or summary(output), '2')}"
    print(line, flush=True)


def main() -> int:
    failures: dict[str, str] = {}

    for name, cmd in (("sync", ("uv", "sync", "--locked")), ("erase", (*RUN, "coverage", "erase"))):
        ok, output, seconds = timed(partial(run, cmd))
        if not ok:
            report(name, ok, output, seconds)
            print(output)
            return 1

    print(paint(f"running {len(GATES)} gates…", "2"), flush=True)
    with ThreadPoolExecutor(max_workers=len(GATES)) as pool:
        # map() yields in GATES order, so lines print in a fixed order as each one finishes.
        results = pool.map(timed, GATES.values())
        for name, (ok, output, seconds) in zip(GATES, results, strict=True):
            report(name, ok, output, seconds)
            if not ok:
                failures[name] = output

    if any(name.startswith("py") for name in failures):
        print(paint("skip coverage  (test failures)", "33"))
    else:
        start = time.monotonic()
        combined, output = run((*RUN, "coverage", "combine", "-q"))
        if combined:
            combined, output = run((*RUN, "coverage", "report"))
        total = re.search(r"^TOTAL.*?(\d+(?:\.\d+)?%)$", output, re.MULTILINE)
        detail = total and f"{total[1]} (all versions)"
        report("coverage", combined, output, time.monotonic() - start, detail)
        if not combined:
            failures["coverage"] = output

    for name, output in failures.items():
        print(f"\n{paint(f'── {name} ', '31;1'):─<70}\n{output.rstrip()}")
    print(paint(f"\n{len(failures)} failed", "31;1") if failures else paint("\nall passed", "32;1"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
