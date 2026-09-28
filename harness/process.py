"""Subprocesses with file-backed output and guaranteed teardown."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: str
    stderr: str


class Process:
    def __init__(
        self,
        command: Sequence[str],
        *,
        env: Mapping[str, str | None] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        output_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self._closed = False
        self._temporary = (
            tempfile.TemporaryDirectory(prefix="lcq-process-") if output_dir is None else None
        )
        directory = Path(self._temporary.name if self._temporary else str(output_dir))
        directory.mkdir(parents=True, exist_ok=True)
        # Exclusive, distinct files even when several children share an artifact directory.
        stdout_fd, stdout_path = tempfile.mkstemp(prefix="stdout-", suffix=".log", dir=directory)
        stderr_fd, stderr_path = tempfile.mkstemp(prefix="stderr-", suffix=".log", dir=directory)
        self.stdout_path = Path(stdout_path)
        self.stderr_path = Path(stderr_path)
        child_env = os.environ.copy()
        for key, value in (env or {}).items():
            if value is None:
                child_env.pop(key, None)
            else:
                child_env[key] = value
        with os.fdopen(stdout_fd, "wb") as stdout, os.fdopen(stderr_fd, "wb") as stderr:
            try:
                self.process = subprocess.Popen(
                    command,
                    cwd=cwd,
                    env=child_env,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                )
            except BaseException:
                if self._temporary is not None:
                    self._temporary.cleanup()
                raise

    def send_signal(self, signum: int) -> None:
        self.process.send_signal(signum)

    def wait(self, timeout: float = 10) -> ProcessResult:
        returncode = self.process.wait(timeout=timeout)
        return ProcessResult(
            returncode,
            self.stdout_path.read_text(errors="replace"),
            self.stderr_path.read_text(errors="replace"),
        )

    def close(self) -> None:
        if self._closed:
            return
        # Kill the process group as well: child helpers must not escape fixture teardown.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGKILL)
        self.process.wait()
        if self._temporary is not None:
            self._temporary.cleanup()
        self._closed = True

    def __enter__(self) -> Process:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
