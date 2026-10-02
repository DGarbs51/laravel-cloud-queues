"""The mode-aware telemetry facade."""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from typing import Protocol

from laravel_cloud_logging import CloudHandler, MonologFormatter

from ._events import sanitize
from ._guard import begin_call, end_call, log_failure

# Same bound as the socket write timeout, so a SIGALRM handler cannot deadlock
# forever on a log lock held by the interrupted thread.
_LOG_LOCK_TIMEOUT_SECONDS = 2.0
"""The number of seconds to wait for the log lock."""
_LOGGER_NAME = "laravel_cloud_queues.worker"
"""The logger name recorded on worker log lines."""


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

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Discard the event."""
        return True

    def close(self) -> None:
        """Close the sink."""
        return None


class Telemetry:
    """The mode-aware telemetry facade used by the dispatch pipeline and the worker.

    ``emit`` sends Cloud lifecycle events only when ``emits_cloud_events`` is set, which
    is the case in managed mode. ``log_line`` writes job and failure records as Laravel
    Cloud log lines and is never gated.
    """

    def __init__(self, *, sink: EventSink, emits_cloud_events: bool) -> None:
        """Create a new telemetry instance."""
        self._sink = sink
        self._emits_cloud_events = emits_cloud_events
        self._lock = threading.Lock()
        # A private handler, so these records reach the platform whatever the app's
        # logging configuration is: the Cloud log socket, else stdout.
        self._handler = CloudHandler()
        self._handler.setFormatter(MonologFormatter())

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

    def log_line(
        self,
        record: Mapping[str, object],
        *,
        message: str,
        level: int = logging.INFO,
        exception: BaseException | None = None,
        lock_timeout: float | None = None,
    ) -> None:
        """Write the record as one Laravel Cloud log line, with its fields in ``context``.

        The line goes to the Cloud log socket, or to stdout off Cloud or when the socket
        fails. The wait for the log lock is bounded and this method never raises. When a
        ``lock_timeout`` is given, the line is written directly to the stdout descriptor,
        without the handler's lock or socket.
        """
        from ._guard import raw_diagnostic, raw_write

        if not begin_call():
            return
        diagnostic = log_failure if lock_timeout is None else raw_diagnostic
        acquired = False
        try:
            acquired = self._lock.acquire(
                timeout=_LOG_LOCK_TIMEOUT_SECONDS if lock_timeout is None else lock_timeout
            )
            if not acquired:
                diagnostic("observability log lock timed out")
                return
            try:
                entry = logging.LogRecord(
                    _LOGGER_NAME,
                    level,
                    __file__,
                    0,
                    message,
                    None,
                    None
                    if exception is None
                    else (type(exception), exception, exception.__traceback__),
                )
                entry.__dict__.update({key: sanitize(value) for key, value in record.items()})
            except Exception:
                diagnostic("observability log record failed")
                return
            if lock_timeout is None:
                self._handler.handle(entry)
                return
            # MonologFormatter.format never raises.
            raw_write(1, (self._handler.format(entry) + "\n").encode("utf-8"), timeout=lock_timeout)
        finally:
            if acquired:
                self._lock.release()
            end_call()
