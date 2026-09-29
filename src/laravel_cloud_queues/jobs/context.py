"""The runtime context of the job delivery being handled.

The context is a runtime object: it is never serialized and never supplied from payload data.
It is injected into handler parameters annotated ``JobContext``, and FastAPI handlers may
resolve it with ``Depends(current_job)``.
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
    """The outcome a handler explicitly chose for its delivery."""

    kind: Literal["release", "fail"]
    """The kind of outcome that was chosen."""
    delay: int = 0
    """The whole seconds to wait before a release, rounded up and clamped to 43,200."""
    reason: BaseException | None = None
    """The exception recorded in the failure record when the job was failed."""


class JobControl(Exception):
    """The exception thrown to stop a handler once it has chosen an outcome.

    It is raised by :meth:`JobContext.release` and :meth:`JobContext.fail`. If user code
    swallows it, the recorded outcome still wins over success.
    """


class JobContext:
    """The context of a single job delivery.

    Calling ``release`` or ``fail`` records the outcome without performing any I/O inside the
    handler. The first call wins; later calls raise again without changing it.
    """

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
        """Create a new job context instance."""
        self._job_name = job_name
        self._uuid = uuid
        self._message_id = message_id
        self._queue = queue
        self._attempt = attempt
        self._max_tries = max_tries
        self._outcome: ChosenOutcome | None = None

    def __repr__(self) -> str:
        """Get the string representation of the job context."""
        return (
            f"JobContext(job_name={self._job_name!r}, uuid={self._uuid!r}, "
            f"queue={self._queue!r}, attempt={self._attempt}, max_tries={self._max_tries})"
        )

    @property
    def job_name(self) -> str:
        """Get the name of the job."""
        return self._job_name

    @property
    def uuid(self) -> str:
        """Get the UUID of the dispatched job."""
        return self._uuid

    @property
    def message_id(self) -> str:
        """Get the transport's identifier for the message."""
        return self._message_id

    @property
    def queue(self) -> str:
        """Get the name of the queue the job was delivered from."""
        return self._queue

    @property
    def attempt(self) -> int:
        """Get the number of times the job has been attempted, including this one."""
        return self._attempt

    @property
    def max_tries(self) -> int:
        """Get the maximum number of attempts allowed for the job."""
        return self._max_tries

    @property
    def outcome(self) -> ChosenOutcome | None:
        """Get the outcome chosen by the handler, if any."""
        return self._outcome

    def release(self, delay: float | timedelta = 0) -> NoReturn:
        """Release the job back onto the queue.

        The job is always released, as in Laravel. The release consumes an attempt like any
        delivery, so once attempts are exhausted the next delivery fails the pre-run check
        with a ``MaxAttemptsExceededError``.
        """
        if self._outcome is None:
            self._outcome = ChosenOutcome("release", delay=normalize_release_delay(delay))
        raise JobControl(f"Job outcome chosen: {self._outcome.kind}.")

    def fail(self, reason: str | BaseException | None = None) -> NoReturn:
        """Mark the job as failed, regardless of any remaining attempts."""
        if self._outcome is None:
            if reason is None:
                reason = JobFailedError("Job failed explicitly.")
            elif isinstance(reason, str):
                reason = JobFailedError(reason)
            self._outcome = ChosenOutcome("fail", reason=reason)
        raise JobControl(f"Job outcome chosen: {self._outcome.kind}.")


CURRENT_JOB: ContextVar[JobContext] = ContextVar("laravel_cloud_queues.current_job")
"""The context of the job running in the current execution context."""


def current_job() -> JobContext:
    """Get the context of the job that is currently running.

    The context is read from a context variable, and a ``RuntimeError`` is raised when no job
    is running. It may be used as a FastAPI dependency via ``Depends(current_job)``.
    """
    try:
        return CURRENT_JOB.get()
    except LookupError:
        raise RuntimeError("current_job() was called outside a running job.") from None
