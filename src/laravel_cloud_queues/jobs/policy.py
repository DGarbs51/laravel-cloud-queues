"""The retry policy of a job and the worker defaults it falls back to.

The policy travels in the envelope. Worker defaults apply only to fields the message omits.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from ..errors import ConfigurationError, InvalidQueueOptionError
from ..transports.base import MAX_FRESH_DELAY_SECONDS, MAX_VISIBILITY_SECONDS

DEFAULT_TRIES = 1
"""The default number of times a job may be attempted."""
DEFAULT_BACKOFF: tuple[float, ...] = (0,)
"""The default number of seconds to wait before retrying a job."""
DEFAULT_TIMEOUT = 60.0
"""The default number of seconds a job may run."""
MAX_JOB_TIMEOUT = 604_800
"""The maximum number of seconds a job may run, which is seven days.

Larger values overflow ``setitimer``, while ``0`` still disables the timeout.
"""


def _is_seconds(value: object) -> bool:
    """Determine if the value is a finite, non-negative int or float, excluding bools."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _check_seconds(field: str, value: object) -> None:
    """Ensure the given field is a finite, non-negative number of seconds."""
    if not _is_seconds(value):
        raise ConfigurationError(f"{field} must be a finite number >= 0, got {value!r}.")


def _check_timeout(value: object) -> None:
    """Ensure the given timeout is valid and no greater than ``MAX_JOB_TIMEOUT``."""
    _check_seconds("timeout", value)
    if isinstance(value, (int, float)) and value > MAX_JOB_TIMEOUT:
        raise ConfigurationError(
            f"timeout must be at most {MAX_JOB_TIMEOUT} seconds (7 days), got {value!r}."
        )


@dataclass(frozen=True)
class RetryPolicy:
    """The retry policy declared by a job.

    Fields left as ``None`` are omitted from the envelope and fall back to worker defaults.
    """

    tries: int | None = None
    """The number of times the job may be attempted, where ``0`` means unlimited."""
    backoff: float | Sequence[float] | None = None
    """The seconds to wait before retrying, either one value or one per attempt."""
    timeout: float | None = None
    """The number of seconds the job may run, where ``0`` disables the timeout."""
    fail_on_timeout: bool | None = None
    """Indicates if the job should be marked as failed when it times out."""

    def __post_init__(self) -> None:
        """Ensure the policy is valid when it is declared.

        Tries must be at least 0, backoff values must be finite and at least 0, and the
        timeout must be finite, at least 0 and no greater than ``MAX_JOB_TIMEOUT``. An
        invalid policy raises a :class:`ConfigurationError`.
        """
        # Runtime checks for untyped callers: fields are read as ``object``.
        tries: object = self.tries
        if tries is not None and (
            not isinstance(tries, int) or isinstance(tries, bool) or tries < 0
        ):
            raise ConfigurationError(f"tries must be an integer >= 0, got {tries!r}.")
        backoff: object = self.backoff
        if isinstance(backoff, Sequence):
            if isinstance(backoff, str) or not backoff:
                raise ConfigurationError(
                    f"backoff must be a number or a non-empty list, got {backoff!r}."
                )
            # A tuple keeps the frozen policy hashable and immune to caller mutation.
            object.__setattr__(self, "backoff", tuple(backoff))
            for value in backoff:
                _check_seconds("backoff", value)
        elif backoff is not None:
            _check_seconds("backoff", backoff)
        if self.timeout is not None:
            _check_timeout(self.timeout)
        fail_on_timeout: object = self.fail_on_timeout
        if fail_on_timeout is not None and not isinstance(fail_on_timeout, bool):
            raise ConfigurationError(f"fail_on_timeout must be a bool, got {fail_on_timeout!r}.")

    def resolve(self, defaults: WorkerDefaults) -> ResolvedPolicy:
        """Resolve the policy, using the worker defaults for any omitted fields."""
        backoff = self.backoff
        return ResolvedPolicy(
            tries=defaults.tries if self.tries is None else self.tries,
            backoff=(
                defaults.backoff
                if backoff is None
                else tuple(backoff)
                if isinstance(backoff, Sequence)
                else (backoff,)
            ),
            timeout=defaults.timeout if self.timeout is None else self.timeout,
            fail_on_timeout=(
                defaults.fail_on_timeout if self.fail_on_timeout is None else self.fail_on_timeout
            ),
        )


@dataclass(frozen=True)
class WorkerDefaults:
    """The worker's policy defaults for fields a message omits."""

    tries: int = DEFAULT_TRIES
    """The number of times a job may be attempted."""
    backoff: tuple[float, ...] = DEFAULT_BACKOFF
    """The seconds to wait before each retry."""
    timeout: float = DEFAULT_TIMEOUT
    """The number of seconds a job may run."""
    fail_on_timeout: bool = False
    """Indicates if a job should be marked as failed when it times out."""

    def __post_init__(self) -> None:
        """Ensure the default timeout is valid."""
        _check_timeout(self.timeout)


@dataclass(frozen=True)
class ResolvedPolicy:
    """The effective retry policy for a single delivery."""

    tries: int
    """The number of times the job may be attempted, where ``0`` means unlimited."""
    backoff: tuple[float, ...]
    """The seconds to wait before each retry, with the last value repeating."""
    timeout: float
    """The number of seconds the job may run, where ``0`` disables the timeout."""
    fail_on_timeout: bool
    """Indicates if the job should be marked as failed when it times out."""

    def exceeded_before_run(self, attempt: int) -> bool:
        """Determine if the attempt exceeds the allowed tries before the job runs.

        This is Laravel's pre-run check: ``tries > 0 and attempt > tries``.
        """
        return self.tries > 0 and attempt > self.tries

    def is_last_attempt(self, attempt: int) -> bool:
        """Determine if the given attempt is the last one allowed.

        A failure on the last attempt is terminal: ``tries > 0 and attempt >= tries``.
        """
        return self.tries > 0 and attempt >= self.tries

    def retry_delay(self, attempt: int) -> int:
        """Get the number of seconds to wait before retrying the given attempt.

        The delay is ``backoff[attempt - 1]``, with the last value repeating. Positive
        fractions round up to whole seconds (0 stays 0), following Symfony rather than
        Laravel, and the result is clamped to 43,200.
        """
        if not self.backoff:
            return 0
        index = min(max(attempt, 1), len(self.backoff)) - 1
        return min(math.ceil(self.backoff[index]), MAX_VISIBILITY_SECONDS)


def _delay_seconds(delay: float | timedelta) -> int:
    """Convert the delay to whole seconds, rounding positive fractions up.

    Raises an :class:`InvalidQueueOptionError` if the delay is negative, not finite or not
    a number.
    """
    seconds = delay.total_seconds() if isinstance(delay, timedelta) else delay
    if not _is_seconds(seconds):
        raise InvalidQueueOptionError(
            f"Delay must be a finite number of seconds >= 0 or a timedelta, got {delay!r}."
        )
    return math.ceil(seconds)


def normalize_delay(delay: float | timedelta) -> int:
    """Normalize the delay of a new dispatch to whole seconds, rounding fractions up.

    Raises an :class:`InvalidQueueOptionError` if the delay is negative, not finite or over
    900 seconds, where Laravel would forward it to the queue unchanged.
    """
    seconds = _delay_seconds(delay)
    if seconds > MAX_FRESH_DELAY_SECONDS:
        raise InvalidQueueOptionError(
            f"Delay of {delay!r} exceeds the {MAX_FRESH_DELAY_SECONDS}-second maximum for a "
            "new message."
        )
    return seconds


def normalize_release_delay(delay: float | timedelta) -> int:
    """Normalize the delay of a release to whole seconds, rounded up and clamped to 43,200.

    Raises an :class:`InvalidQueueOptionError` if the delay is negative or not finite.
    """
    return min(_delay_seconds(delay), MAX_VISIBILITY_SECONDS)
