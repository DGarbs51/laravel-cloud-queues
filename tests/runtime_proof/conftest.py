"""Local pytest config for the runtime proof suite."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from support import TIMINGS, LineCollector, calibrate_native_iterations  # noqa: E402


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "runtime_proof: subprocess proofs of process-level timeouts and watchdog renewal",
    )


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, exitstatus: int, config: pytest.Config) -> None:
    if not TIMINGS:
        return
    terminalreporter.write_sep("-", "runtime proof timings")
    for line in TIMINGS:
        terminalreporter.write_line(line)


@pytest.fixture(scope="session")
def native_iterations() -> int:
    return calibrate_native_iterations(1.35)


@pytest.fixture
def collector() -> LineCollector:
    recording = LineCollector()
    recording.start()
    try:
        yield recording
    finally:
        recording.stop()
