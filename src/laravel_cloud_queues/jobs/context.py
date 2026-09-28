"""``JobContext`` (PROJECT_SCOPE.md §12). CONTRACT — implemented by lane L3c.

A runtime object: never serializable, never supplied from payload data. Core injects it into
handler parameters annotated ``JobContext``; FastAPI exposes it via ``Depends(current_job)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, NoReturn


@dataclass(frozen=True)
class ChosenOutcome:
    kind: Literal["release", "fail"]
    delay: int = 0
    """Whole seconds for ``release`` (rounded up, clamped to 43,200)."""
    reason: BaseException | None = None
    """For ``fail``: the exception recorded in the failure record."""


class JobControl(Exception):
    """Raised by :meth:`JobContext.release`/:meth:`JobContext.fail` to short-circuit the
    handler. If user code swallows it, the recorded outcome still wins over success."""


class JobContext:
    """Per-delivery context. ``release``/``fail`` record the outcome (first call wins; later
    calls re-raise without changing it) and never perform I/O inside the handler."""

    def __init__(
        self,
        *,
        job_name: str,
        uuid: str,
        message_id: str,
        queue: str,
        attempt: int,
        max_tries: int,
    ) -> None:
        raise NotImplementedError

    @property
    def job_name(self) -> str:
        raise NotImplementedError

    @property
    def uuid(self) -> str:
        raise NotImplementedError

    @property
    def message_id(self) -> str:
        raise NotImplementedError

    @property
    def queue(self) -> str:
        raise NotImplementedError

    @property
    def attempt(self) -> int:
        raise NotImplementedError

    @property
    def max_tries(self) -> int:
        raise NotImplementedError

    @property
    def outcome(self) -> ChosenOutcome | None:
        raise NotImplementedError

    def release(self, delay: float | timedelta = 0) -> NoReturn:
        """Release this delivery for retry (always released, Laravel parity). It consumes an
        attempt like any delivery: when attempts are exhausted the NEXT delivery fails the
        pre-run check (``MaxAttemptsExceededError``)."""
        raise NotImplementedError

    def fail(self, reason: str | BaseException | None = None) -> NoReturn:
        """Fail terminally now, regardless of remaining attempts."""
        raise NotImplementedError


def current_job() -> JobContext:
    """The ``JobContext`` of the delivery running in this context (contextvar).
    Outside a job -> RuntimeError. Usable as a FastAPI dependency: ``Depends(current_job)``."""
    raise NotImplementedError
