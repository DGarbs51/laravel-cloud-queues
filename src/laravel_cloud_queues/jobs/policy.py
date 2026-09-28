"""Retry policy (PROJECT_SCOPE.md §12, D4). CONTRACT — math implemented by lane L3c.

The policy travels in the envelope. Worker defaults apply only to fields the message omits.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

DEFAULT_TRIES = 1
DEFAULT_BACKOFF: tuple[float, ...] = (0,)
DEFAULT_TIMEOUT = 60.0


@dataclass(frozen=True)
class RetryPolicy:
    """Declared policy. ``None`` fields are omitted from the envelope and fall back to worker
    defaults. ``tries=0`` = unlimited. ``timeout=0`` disables the timeout."""

    tries: int | None = None
    backoff: float | Sequence[float] | None = None
    timeout: float | None = None
    fail_on_timeout: bool | None = None

    def __post_init__(self) -> None:
        """Validate: tries >= 0; backoff values finite and >= 0; timeout finite and >= 0.
        Invalid -> ConfigurationError at declaration time."""
        raise NotImplementedError

    def resolve(self, defaults: WorkerDefaults) -> ResolvedPolicy:
        raise NotImplementedError


@dataclass(frozen=True)
class WorkerDefaults:
    tries: int = DEFAULT_TRIES
    backoff: tuple[float, ...] = DEFAULT_BACKOFF
    timeout: float = DEFAULT_TIMEOUT
    fail_on_timeout: bool = False


@dataclass(frozen=True)
class ResolvedPolicy:
    tries: int
    backoff: tuple[float, ...]
    timeout: float
    fail_on_timeout: bool

    def exceeded_before_run(self, attempt: int) -> bool:
        """``tries > 0 and attempt > tries`` (Laravel pre-run check)."""
        raise NotImplementedError

    def is_last_attempt(self, attempt: int) -> bool:
        """``tries > 0 and attempt >= tries`` (terminal after exception / timeout)."""
        raise NotImplementedError

    def retry_delay(self, attempt: int) -> int:
        """``backoff[attempt - 1]``, last value repeating; positive fractions round UP to whole
        seconds (0 stays 0); clamped to 43,200. (Symfony rounding; labeled deviation.)"""
        raise NotImplementedError


def normalize_delay(delay: float | timedelta) -> int:
    """Fresh-dispatch delay -> whole seconds. Positive fractions round up. Negative,
    non-finite or > 900 -> InvalidQueueOptionError (labeled deviation: Laravel forwards)."""
    raise NotImplementedError


def normalize_release_delay(delay: float | timedelta) -> int:
    """``JobContext.release(delay)`` -> whole seconds, rounded up, clamped to 43,200.
    Negative / non-finite -> InvalidQueueOptionError."""
    raise NotImplementedError
