"""The mode-aware telemetry facade."""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from typing import Protocol

import anyio.to_thread

from ._events import sanitize
from ._guard import begin_call, end_call, log_failure

# Same bound as the socket write timeout, so a SIGALRM handler cannot deadlock
# forever on a log lock held by the interrupted thread.
_LOG_LOCK_TIMEOUT_SECONDS = 2.0
"""The number of seconds to wait for the log lock."""
_logger: logging.Logger = logging.getLogger("laravel_cloud_queues.worker")
"""The logger that receives job and failure records."""


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
    is the case in managed mode. ``log_line`` logs job and failure records through
    ``laravel-cloud-logging`` in every mode.
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

    async def aemit(self, event: Mapping[str, object]) -> None:
        """Send the Cloud event to the sink from a worker thread, off the event loop.

        This does nothing (and never leaves the loop) unless ``emits_cloud_events`` is set,
        and never raises.
        """
        if not self._emits_cloud_events:
            return
        try:
            await anyio.to_thread.run_sync(self.emit, event)
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
        """Log the record on the worker logger, with its fields as ``extra``.

        ``laravel-cloud-logging`` puts the fields in ``context``. The wait for the log lock is
        bounded and this method never raises. When a ``lock_timeout`` is given, the line is
        written directly to the stdout descriptor in the same format, without logging locks.
        """
        from ._guard import raw_diagnostic, raw_log

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
            exc_info = (
                None if exception is None else (type(exception), exception, exception.__traceback__)
            )
            try:
                extra = {key: sanitize(value) for key, value in record.items()}
                if lock_timeout is None:
                    _logger.log(level, "%s", message, exc_info=exc_info, extra=extra)
                    return
                entry = _logger.makeRecord(
                    _logger.name, level, __file__, 0, message, (), exc_info, extra=extra
                )
            except Exception:
                diagnostic("observability log record failed")
                return
            raw_log(entry, timeout=lock_timeout)
        finally:
            if acquired:
                self._lock.release()
            end_call()
