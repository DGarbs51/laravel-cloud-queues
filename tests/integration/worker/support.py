"""Helpers for worker subprocess tests: run the real CLI, read its stdout records and the
jobs' own run records."""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.harness.process import Process, ProcessResult

ROOT = Path(__file__).resolve().parents[3]
TARGET = "tests.integration.worker.apps.basic:registry"
_AMBIENT = ("AWS_", "LARAVEL_CLOUD_", "REDIS_URL", "LCQ_")


def clean_env(values: Mapping[str, str]) -> dict[str, str | None]:
    """Drop ambient AWS/Cloud/Redis settings; tests pass everything explicitly."""
    env: dict[str, str | None] = {k: None for k in os.environ if k.startswith(_AMBIENT)}
    env.update(values)
    return env


def json_lines(text: str) -> list[dict[str, Any]]:
    lines = []
    for raw in text.splitlines():
        try:
            value = json.loads(raw)
        except ValueError:
            continue
        if isinstance(value, dict):
            lines.append(value)
    return lines


@dataclass
class Run:
    result: ProcessResult
    elapsed: float

    @property
    def code(self) -> int:
        return self.result.returncode

    @property
    def job_lines(self) -> list[dict[str, Any]]:
        return [
            line
            for line in json_lines(self.result.stdout)
            if line.get("laravel_cloud_queues") == "job"
        ]

    @property
    def statuses(self) -> list[str]:
        return [line["status"] for line in self.job_lines]

    @property
    def failure_records(self) -> list[dict[str, Any]]:
        return [
            line
            for line in json_lines(self.result.stdout)
            if line.get("laravel_cloud_queues") != "job" and "payload" in line
        ]

    def describe(self) -> str:
        return f"exit {self.code}\nstdout:\n{self.result.stdout}\nstderr:\n{self.result.stderr}"


ProcessFactory = Callable[..., Process]


@dataclass
class Workers:
    """Starts ``laravel-cloud-queues work`` subprocesses with a fixed environment."""

    run_process: ProcessFactory
    env: dict[str, str | None]
    record_path: Path
    started: list[tuple[Process, float]] = field(default_factory=list)

    def start(self, *args: str, lease: int | None = None, target: str = TARGET) -> Process:
        if lease is None:
            command = [sys.executable, "-m", "laravel_cloud_queues.cli", "work", target, *args]
        else:
            module = "tests.integration.worker.apps.short_lease"
            command = [sys.executable, "-m", module, str(lease), "work", target, *args]
        process = self.run_process(command, env=self.env, cwd=ROOT)
        self.started.append((process, time.monotonic()))
        return process

    def wait(self, process: Process, timeout: float = 60) -> Run:
        began = next(at for p, at in self.started if p is process)
        result = process.wait(timeout=timeout)
        return Run(result, time.monotonic() - began)

    def run(self, *args: str, lease: int | None = None, timeout: float = 60) -> Run:
        return self.wait(self.start(*args, lease=lease), timeout=timeout)

    def records(self) -> list[dict[str, Any]]:
        if not self.record_path.exists():
            return []
        return json_lines(self.record_path.read_text())

    def wait_for_record(
        self, predicate: Callable[[dict[str, Any]], bool], timeout: float = 30
    ) -> None:
        deadline = time.monotonic() + timeout
        while not any(predicate(r) for r in self.records()):
            if time.monotonic() > deadline:
                raise TimeoutError("record not written")
            time.sleep(0.05)

    def wait_until_idle(self, process: Process, timeout: float = 30) -> None:
        """Until the worker logged its start (it then blocks in receive)."""
        deadline = time.monotonic() + timeout
        while "Worker started" not in process.stderr_path.read_text(errors="replace"):
            if process.process.poll() is not None or time.monotonic() > deadline:
                raise TimeoutError(process.stderr_path.read_text(errors="replace"))
            time.sleep(0.05)
        time.sleep(0.5)

    def terminate(self, process: Process) -> float:
        process.send_signal(signal.SIGTERM)
        return time.monotonic()


def native_iterations(seconds: float) -> int:
    """``sum(range(n))`` runs in C without checking for signals and holds the GIL."""
    sample = 5_000_000
    started = time.perf_counter()
    sum(range(sample))
    return int(sample * seconds / max(time.perf_counter() - started, 1e-6))
