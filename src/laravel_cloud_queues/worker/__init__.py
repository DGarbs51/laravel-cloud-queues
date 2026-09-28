"""The queue worker (PROJECT_SCOPE.md §11-§14). CONTRACT — implemented by lane L6.

State machine per delivery (exactly one outcome owner):
received -> [decode: defect => terminal] -> [pre-run attempt check] -> running
-> outcome chosen (success | release | fail | error->retry/terminal | timeout)
-> reporting (transport complete/release) -> completed | ambiguous(stop).
See docs/contract/worker.md.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..registry import WorkerTarget

EXIT_OK = 0
EXIT_FATAL = 1
EXIT_CONFIG = 2
EXIT_TIMEOUT = 124


@dataclass(frozen=True)
class WorkerOptions:
    queues: tuple[str, ...] | None = None
    """Priority list; ``None`` = backend default / Cloud assignment."""
    max_jobs: int | None = None
    max_time: float | None = None
    stop_when_empty: bool = False
    stop_when_empty_for: float | None = None
    timeout: float = 60.0
    """Default job timeout when the message omits one (0 disables)."""
    sleep: float = 3.0
    rest: float = 0.0
    lease_seconds: int = 60
    """Visibility/reservation lease renewed every third by the watchdog (D7)."""


class Worker:
    def __init__(self, target: WorkerTarget, options: WorkerOptions | None = None) -> None:
        raise NotImplementedError

    def run(self) -> int:
        """Run until a stop condition; returns the process exit code (0/1/2; 124 exits via
        ``os._exit`` from the timeout handler). Must run on the main thread."""
        raise NotImplementedError


def resolve_target(spec: str) -> WorkerTarget:
    """``module:attr`` -> WorkerTarget: a Registry, an object exposing ``registry`` and
    ``lifespan()``, or an app whose ``state.laravel_cloud_queues`` is one. ConfigurationError
    otherwise. Never imports fastapi itself."""
    raise NotImplementedError


__all__ = [
    "EXIT_CONFIG",
    "EXIT_FATAL",
    "EXIT_OK",
    "EXIT_TIMEOUT",
    "Worker",
    "WorkerOptions",
    "resolve_target",
]
