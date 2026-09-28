"""``JobContext`` (PROJECT_SCOPE.md §12). CONTRACT — implemented by lane L3c.

A runtime object: never serializable, never supplied from payload data. Core injects it into
handler parameters annotated ``JobContext``; FastAPI exposes it via ``Depends(current_job)``.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal, NoReturn

from ..errors import JobFailedError
from .policy import normalize_release_delay


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
        self._job_name = job_name
        self._uuid = uuid
        self._message_id = message_id
        self._queue = queue
        self._attempt = attempt
        self._max_tries = max_tries
        self._outcome: ChosenOutcome | None = None

    def __repr__(self) -> str:
        return (
            f"JobContext(job_name={self._job_name!r}, uuid={self._uuid!r}, "
            f"queue={self._queue!r}, attempt={self._attempt}, max_tries={self._max_tries})"
        )

    @property
    def job_name(self) -> str:
        return self._job_name

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def message_id(self) -> str:
        return self._message_id

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def max_tries(self) -> int:
        return self._max_tries

    @property
    def outcome(self) -> ChosenOutcome | None:
        return self._outcome

    def release(self, delay: float | timedelta = 0) -> NoReturn:
        """Release this delivery for retry (always released, Laravel parity). It consumes an
        attempt like any delivery: when attempts are exhausted the NEXT delivery fails the
        pre-run check (``MaxAttemptsExceededError``)."""
        if self._outcome is None:
            self._outcome = ChosenOutcome("release", delay=normalize_release_delay(delay))
        raise JobControl(f"Job outcome chosen: {self._outcome.kind}.")

    def fail(self, reason: str | BaseException | None = None) -> NoReturn:
        """Fail terminally now, regardless of remaining attempts."""
        if self._outcome is None:
            if reason is None:
                reason = JobFailedError("Job failed explicitly.")
            elif isinstance(reason, str):
                reason = JobFailedError(reason)
            self._outcome = ChosenOutcome("fail", reason=reason)
        raise JobControl(f"Job outcome chosen: {self._outcome.kind}.")


_current_job: ContextVar[JobContext] = ContextVar("laravel_cloud_queues.current_job")


def current_job() -> JobContext:
    """The ``JobContext`` of the delivery running in this context (contextvar).
    Outside a job -> RuntimeError. Usable as a FastAPI dependency: ``Depends(current_job)``."""
    try:
        return _current_job.get()
    except LookupError:
        raise RuntimeError("current_job() was called outside a running job.") from None
