"""Mode-aware telemetry facade (D12, D6b)."""

from __future__ import annotations

import sys
import threading
from collections.abc import Mapping
from typing import Protocol

from ._events import encode_event_line
from ._guard import begin_call, end_call, log_failure

# Same bound as the socket write timeout, so a SIGALRM handler cannot deadlock
# forever on a stdout lock held by the interrupted thread.
_STDOUT_LOCK_TIMEOUT_SECONDS = 2.0


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


class Telemetry:
    """Mode-aware facade used by the dispatch pipeline and the worker.

    ``emit`` sends Cloud lifecycle events only when ``emits_cloud_events`` is set
    (managed mode, D12). ``log_line`` is the D6b stdout path and is not gated.
    """

    def __init__(self, *, sink: EventSink, emits_cloud_events: bool) -> None:
        self._sink = sink
        self._emits_cloud_events = emits_cloud_events
        self._lock = threading.Lock()

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        """No-op unless ``emits_cloud_events``."""

        if not self._emits_cloud_events:
            return
        try:
            self._sink.emit(event, lock_timeout=lock_timeout)
        except Exception:
            log_failure("observability emit failed")

    def log_line(self, record: Mapping[str, object]) -> None:
        """One JSON line to stdout (flush); never raises."""

        if not begin_call():
            return
        acquired = False
        try:
            acquired = self._lock.acquire(timeout=_STDOUT_LOCK_TIMEOUT_SECONDS)
            if not acquired:
                log_failure("observability stdout lock timed out")
                return
            try:
                line = encode_event_line(record)
            except Exception:
                log_failure("observability stdout encoding failed")
                return
            try:
                sys.stdout.write(line.decode("utf-8"))
                sys.stdout.flush()
            except Exception:
                log_failure("observability stdout write failed")
        finally:
            if acquired:
                self._lock.release()
            end_call()
