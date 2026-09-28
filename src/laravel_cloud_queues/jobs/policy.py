"""Retry policy (PROJECT_SCOPE.md §12, D4). CONTRACT — math implemented by lane L3c.

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
DEFAULT_BACKOFF: tuple[float, ...] = (0,)
DEFAULT_TIMEOUT = 60.0
MAX_JOB_TIMEOUT = 604_800
"""7 days. Larger values overflow ``setitimer``; ``0`` still disables the timeout."""


def _is_seconds(value: object) -> bool:
    """A finite, non-negative int or float (bool excluded)."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _check_seconds(field: str, value: object) -> None:
    if not _is_seconds(value):
        raise ConfigurationError(f"{field} must be a finite number >= 0, got {value!r}.")


def _check_timeout(value: object) -> None:
    _check_seconds("timeout", value)
    if isinstance(value, (int, float)) and value > MAX_JOB_TIMEOUT:
        raise ConfigurationError(
            f"timeout must be at most {MAX_JOB_TIMEOUT} seconds (7 days), got {value!r}."
        )


@dataclass(frozen=True)
class RetryPolicy:
    """Declared policy. ``None`` fields are omitted from the envelope and fall back to worker
    defaults. ``tries=0`` = unlimited. ``timeout=0`` disables the timeout."""

    tries: int | None = None
    backoff: float | Sequence[float] | None = None
    timeout: float | None = None
    fail_on_timeout: bool | None = None

    def __post_init__(self) -> None:
        """Validate: tries >= 0; backoff values finite and >= 0; timeout finite, >= 0 and
        <= ``MAX_JOB_TIMEOUT``.
        Invalid -> ConfigurationError at declaration time."""
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
    tries: int = DEFAULT_TRIES
    backoff: tuple[float, ...] = DEFAULT_BACKOFF
    timeout: float = DEFAULT_TIMEOUT
    fail_on_timeout: bool = False

    def __post_init__(self) -> None:
        _check_timeout(self.timeout)


@dataclass(frozen=True)
class ResolvedPolicy:
    tries: int
    backoff: tuple[float, ...]
    timeout: float
    fail_on_timeout: bool

    def exceeded_before_run(self, attempt: int) -> bool:
        """``tries > 0 and attempt > tries`` (Laravel pre-run check)."""
        return self.tries > 0 and attempt > self.tries

    def is_last_attempt(self, attempt: int) -> bool:
        """``tries > 0 and attempt >= tries`` (terminal after exception / timeout)."""
        return self.tries > 0 and attempt >= self.tries

    def retry_delay(self, attempt: int) -> int:
        """``backoff[attempt - 1]``, last value repeating; positive fractions round UP to whole
        seconds (0 stays 0); clamped to 43,200. (Symfony rounding; labeled deviation.)"""
        if not self.backoff:
            return 0
        index = min(max(attempt, 1), len(self.backoff)) - 1
        return min(math.ceil(self.backoff[index]), MAX_VISIBILITY_SECONDS)


def _delay_seconds(delay: float | timedelta) -> int:
    """Whole seconds, positive fractions rounded up. Negative / non-finite / non-numeric ->
    InvalidQueueOptionError."""
    seconds = delay.total_seconds() if isinstance(delay, timedelta) else delay
    if not _is_seconds(seconds):
        raise InvalidQueueOptionError(
            f"Delay must be a finite number of seconds >= 0 or a timedelta, got {delay!r}."
        )
    return math.ceil(seconds)


def normalize_delay(delay: float | timedelta) -> int:
    """Fresh-dispatch delay -> whole seconds. Positive fractions round up. Negative,
    non-finite or > 900 -> InvalidQueueOptionError (labeled deviation: Laravel forwards)."""
    seconds = _delay_seconds(delay)
    if seconds > MAX_FRESH_DELAY_SECONDS:
        raise InvalidQueueOptionError(
            f"Delay of {delay!r} exceeds the {MAX_FRESH_DELAY_SECONDS}-second maximum for a "
            "new message."
        )
    return seconds


def normalize_release_delay(delay: float | timedelta) -> int:
    """``JobContext.release(delay)`` -> whole seconds, rounded up, clamped to 43,200.
    Negative / non-finite -> InvalidQueueOptionError."""
    return min(_delay_seconds(delay), MAX_VISIBILITY_SECONDS)
