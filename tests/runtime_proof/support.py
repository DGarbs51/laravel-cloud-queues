"""Subprocess helpers for the runtime proof suite."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from broker import Broker

ROOT = Path(__file__).resolve().parent
WORKER = ROOT / "proof_worker.py"
TIMINGS: list[str] = []


def note(scenario: str, **fields: object) -> None:
    detail = " ".join(f"{key}={value}" for key, value in fields.items())
    TIMINGS.append(f"{scenario} {detail}".strip())


def calibrate_native_iterations(target_s: float = 1.35) -> int:
    sample = 3_000_000
    started = time.perf_counter()
    sum(range(sample))
    elapsed = max(time.perf_counter() - started, 1e-4)
    return max(sample, int(sample * target_s / elapsed))


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def spawn(args: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.run(
        [sys.executable, str(WORKER), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def popen(args: list[str]) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.Popen(
        [sys.executable, str(WORKER), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def explain(
    proc: subprocess.CompletedProcess[str] | subprocess.Popen[str],
    stdout: str = "",
    stderr: str = "",
) -> str:
    if isinstance(proc, subprocess.CompletedProcess):
        stdout, stderr = proc.stdout, proc.stderr
        code = proc.returncode
    else:
        code = proc.returncode
    return f"exit={code}\nstdout={stdout}\nstderr={stderr}"


def wait_until_visible(broker: Broker, message_id: str, timeout: float = 6) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = broker.get(message_id)
        if int(row["deleted"]) != 0:
            raise AssertionError(f"message deleted while waiting: {row}")
        if float(row["visibility_until"]) <= time.time():
            return row
        time.sleep(0.02)
    raise AssertionError(f"message stayed hidden: {broker.get(message_id)}")


def wait_for_kind(path: Path, kind: str, timeout: float = 5) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for row in read_events(path):
            if row.get("kind") == kind:
                return row
        time.sleep(0.02)
    raise AssertionError(f"no {kind} event in {path}")


class LineCollector:
    """Unix-stream collector. One NDJSON line per connection."""

    def __init__(self) -> None:
        self.path = f"/tmp/lcq-l2-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"
        self.lines: list[str] = []
        self._ready = threading.Event()
        self._stop = False
        self._srv: socket.socket | None = None
        self._thread = threading.Thread(target=self._serve, name="proof-collector", daemon=True)

    def start(self) -> None:
        self._thread.start()
        if not self._ready.wait(2):
            raise RuntimeError("collector failed to bind")

    def _serve(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv = srv
        srv.bind(self.path)
        srv.listen(8)
        srv.settimeout(0.2)
        self._ready.set()
        while not self._stop:
            try:
                conn, _addr = srv.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            try:
                conn.settimeout(2)
                chunks: list[bytes] = []
                while b"\n" not in b"".join(chunks):
                    block = conn.recv(65536)
                    if not block:
                        break
                    chunks.append(block)
                text = b"".join(chunks).decode()
                if text:
                    self.lines.append(text)
            finally:
                conn.close()

    def stop(self) -> None:
        self._stop = True
        if self._srv is not None:
            try:
                self._srv.close()
            except OSError:
                pass
        self._thread.join(timeout=2)
        if os.path.exists(self.path):
            os.unlink(self.path)
