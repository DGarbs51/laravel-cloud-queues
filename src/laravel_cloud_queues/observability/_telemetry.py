"""The mode-aware telemetry facade."""

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
"""The number of seconds to wait for the stdout lock."""


class EventSink(Protocol):
    """A destination that Cloud events may be written to."""

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Write the event as a single NDJSON line.

        Returns False on failure and never raises. ``lock_timeout`` bounds the wait for the
        write lock; the ``SIGALRM`` timeout path passes a small value so it cannot deadlock
        on a write interrupted in the same thread.
        """
        ...

    def close(self) -> None:
        """Close the sink."""
        ...


class NullSink:
    """An event sink that discards every event."""

    @staticmethod
    def emit(event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Discard the event."""
        return True

    @staticmethod
    def close() -> None:
        """Close the sink."""
        return None


class Telemetry:
    """The mode-aware telemetry facade used by the dispatch pipeline and the worker.

    ``emit`` sends Cloud lifecycle events only when ``emits_cloud_events`` is set, which
    is the case in managed mode. ``log_line`` writes failure records to stdout and is
    never gated.
    """

    def __init__(self, *, sink: EventSink, emits_cloud_events: bool) -> None:
        """Create a new telemetry instance."""
        self._sink = sink
        self._emits_cloud_events = emits_cloud_events
        self._lock = threading.Lock()

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        """Send the Cloud event to the sink.

        This does nothing unless ``emits_cloud_events`` is set, and never raises.
        """

        if not self._emits_cloud_events:
            return
        try:
            self._sink.emit(event, lock_timeout=lock_timeout)
        except Exception:
            log_failure("observability emit failed")

    def log_line(self, record: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        """Write the record to stdout as a single JSON line and flush it.

        The wait for the stdout lock is bounded and this method never raises. When a
        ``lock_timeout`` is given, the line is written directly to the descriptor.
        """
        from ._guard import raw_diagnostic, raw_write

        if not begin_call():
            return
        diagnostic = log_failure if lock_timeout is None else raw_diagnostic
        acquired = False
        try:
            acquired = self._lock.acquire(
                timeout=_STDOUT_LOCK_TIMEOUT_SECONDS if lock_timeout is None else lock_timeout
            )
            if not acquired:
                diagnostic("observability stdout lock timed out")
                return
            try:
                line = encode_event_line(record)
            except Exception:
                diagnostic("observability stdout encoding failed")
                return
            try:
                if lock_timeout is not None:
                    raw_write(1, line, timeout=lock_timeout)
                else:
                    sys.stdout.write(line.decode("utf-8"))
                    sys.stdout.flush()
            except Exception:
                diagnostic("observability stdout write failed")
        finally:
            if acquired:
                self._lock.release()
            end_call()
