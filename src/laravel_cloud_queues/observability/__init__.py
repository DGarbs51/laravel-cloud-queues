"""Laravel Cloud observability (PROJECT_SCOPE.md §15, D1, D12) and tracing (§16).
CONTRACT — implemented by lane L7.

Events go to the log socket ONLY in managed mode (D12). In ``sqs``/``redis`` modes the
worker writes structured JSON lines to its own stdout instead (D6b terminal failures).
Telemetry never raises to the caller and never converts a successful job into a failure.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Literal, Protocol

LifecycleType = Literal["queued", "started", "processed", "released", "failed"]
FAILED_JOB_LINE_LIMIT = 16_384
"""Collector line limit documented by symfony-on-cloud (unverified; D1)."""
EXCEPTION_PREVIEW_LIMIT = 1001


class EventSink(Protocol):
    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Write one NDJSON line. Returns False on failure; never raises. ``lock_timeout``
        bounds the wait for the write lock (the SIGALRM timeout path passes a small value so
        it cannot deadlock on a write interrupted in the same thread)."""
        ...

    def close(self) -> None: ...


class NullSink:
    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        return True

    def close(self) -> None:
        return None


class SocketEventSink:
    """Persistent Unix stream socket writer (Laravel ``Cloud\\Events``): 2 s connect and write
    timeouts, EOF check + reconnect before each write, partial-write loop, give up after 5
    zero-byte writes, one line per event, thread-safe (lines never interleave)."""

    def __init__(self, address: str) -> None:
        """``address``: ``unix:///path`` or a bare path."""
        raise NotImplementedError

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def format_timestamp(moment: datetime) -> str:
    """UTC ``Y-m-d H:i:s.u`` (six-digit microseconds, no ``T``, no zone)."""
    raise NotImplementedError


def encode_event_line(event: Mapping[str, object]) -> bytes:
    """Compact JSON, slashes and Unicode unescaped, zero fractions kept, invalid UTF-8 /
    lone surrogates replaced with U+FFFD, trailing newline."""
    raise NotImplementedError


def lifecycle_event(
    type_: LifecycleType,
    queue: str,
    *,
    timestamp: datetime,
    duration_ms: int | None = None,
) -> dict[str, object]:
    """``duration_ms`` (truncated, >= 0) only for processed/released/failed."""
    raise NotImplementedError


def failed_job_event(
    *,
    queue: str,
    payload: str,
    exception: BaseException,
    attempts: int,
    started_at: datetime,
    timestamp: datetime,
    limit_bytes: int = FAILED_JOB_LINE_LIMIT,
) -> dict[str, object]:
    """Laravel ``FailedJobProvider::log`` field set; ``id`` = UUIDv7 from ``timestamp``;
    ``job_name`` from the payload's ``displayName`` (``""`` if unavailable). D1 size policy:
    whole line fits -> as is; else trim ``exception`` (head + marker); else also trim
    ``payload`` and add ``"replayable": false``. Measured on the encoded line in bytes."""
    raise NotImplementedError


def uuid7(timestamp: datetime) -> str:
    """RFC 9562 UUIDv7 bound to ``timestamp`` (millisecond precision)."""
    raise NotImplementedError


def failure_log_record(
    *,
    queue: str,
    payload: str,
    exception: BaseException,
    attempts: int,
    message_id: str,
    started_at: datetime,
    timestamp: datetime,
) -> dict[str, object]:
    """D6b log-only failure record for ``sqs``/``redis`` modes (one JSON line on stdout)."""
    raise NotImplementedError


class Telemetry:
    """Mode-aware facade used by the dispatch pipeline and the worker."""

    def __init__(self, *, sink: EventSink, emits_cloud_events: bool) -> None:
        raise NotImplementedError

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        """No-op unless ``emits_cloud_events``."""
        raise NotImplementedError

    def log_line(self, record: Mapping[str, object]) -> None:
        """One JSON line to stdout (flush); never raises."""
        raise NotImplementedError


def inject_trace_context() -> dict[str, str]:
    """W3C ``traceparent``/``tracestate`` via OpenTelemetry when importable; else ``{}``."""
    raise NotImplementedError


def activate_trace_context(carrier: Mapping[str, str]) -> AbstractContextManager[None]:
    """Extract + attach for the job; detach afterwards (no leakage between jobs). No-op
    without OpenTelemetry; never raises."""
    raise NotImplementedError
