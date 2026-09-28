"""B1: alarm reporting must not wait indefinitely for logging or telemetry locks."""

from __future__ import annotations

import subprocess
import sys

import pytest


@pytest.mark.subprocess
@pytest.mark.parametrize(
    "case", ["worker", "observability", "stdout", "buffered_stdout", "stderr_pipe"]
)
def test_timeout_exits_124_with_contended_locks(case: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "tests.integration.worker.apps.timeout_safety", case],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 124, result.stderr
    assert result.stdout.count("completed") == 1
