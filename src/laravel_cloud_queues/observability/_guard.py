"""The re-entry guard for observability logging.

A logging handler that writes back into the socket or stdout must not recurse, and
must not deadlock on the sink lock held by the emit that is logging the failure.
"""

from __future__ import annotations

import logging
import os
import select
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager

from laravel_cloud_logging import CloudHandler, MonologFormatter

_logger: logging.Logger = logging.getLogger("laravel_cloud_queues.observability")
"""The logger that receives observability failures."""
_local = threading.local()
"""The per-thread re-entry and signal-safety state."""


def raw_write(fd: int, data: bytes, *, timeout: float = 0) -> None:
    """Write the given bytes directly to the file descriptor, on a best-effort basis.

    This is safe to call from an alarm handler. With a zero ``timeout`` the data is
    written once; otherwise the remainder is drained for up to ``timeout`` seconds.
    Errors are swallowed and the descriptor's blocking mode is restored.
    """
    try:
        blocking = os.get_blocking(fd)
        deadline = time.monotonic() + timeout
        try:
            if blocking:
                os.set_blocking(fd, False)
            pending = memoryview(data)
            while pending:
                try:
                    written = os.write(fd, pending)
                except BlockingIOError:
                    written = 0
                pending = pending[written:]
                if not pending or timeout == 0:
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([], [fd], [], remaining)[1]:
                    return
        finally:
            # Descriptors inherited from a supervisor may share these flags; restore them.
            if blocking:
                os.set_blocking(fd, True)
    except OSError:
        return


def _formatter() -> logging.Formatter:
    """Get the formatter of the root ``laravel-cloud-logging`` handler.

    Falls back to a JSON ``MonologFormatter`` when the root logger has no such handler.
    Reading the handler list takes no lock, so this is safe in an alarm handler.
    """
    for handler in logging.getLogger().handlers:
        if isinstance(handler, CloudHandler) and handler.formatter is not None:
            return handler.formatter
    return MonologFormatter()


def raw_log(record: logging.LogRecord, *, timeout: float = 0) -> None:
    """Write the record to stdout as a ``laravel-cloud-logging`` line, without logging locks.

    The line has the same format as the app's other log lines. This is safe to call from
    an alarm handler, where a handler lock may be held by the interrupted code.
    ``MonologFormatter.format`` never raises.
    """
    line = _formatter().format(record) + "\n"
    raw_write(1, line.encode("utf-8", errors="replace"), timeout=timeout)


def raw_diagnostic(message: str, level: int = logging.WARNING) -> None:
    """Write the diagnostic message as a log line without logging or stream locks.

    This is a single best-effort write that is safe to call from an alarm handler.
    """
    raw_log(
        logging.LogRecord(
            "laravel_cloud_queues.observability", level, __file__, 0, message, None, None
        )
    )


@contextmanager
def signal_safe() -> Generator[None]:
    """Keep telemetry fallback diagnostics off logging locks while handling ``SIGALRM``."""
    previous = getattr(_local, "signal_safe", False)
    _local.signal_safe = True
    try:
        yield
    finally:
        _local.signal_safe = previous


def begin_call() -> bool:
    """Mark the current thread as inside an observability call.

    Returns False when the thread is already inside one, meaning a logging handler
    re-entered the sink. Callers must pair a True result with :func:`end_call`.
    """

    if getattr(_local, "depth", 0):
        return False
    _local.depth = 1
    return True


def end_call() -> None:
    """Clear the re-entry mark for the current thread."""

    _local.depth = 0


def log_failure(message: str) -> None:
    """Log the failure message locally.

    The message never includes payloads, and this method never raises.
    """

    if getattr(_local, "signal_safe", False):
        raw_diagnostic(message)
        return
    if getattr(_local, "in_log", False):
        return
    _local.in_log = True
    try:
        _logger.warning("%s", message)
    except Exception:
        return
    finally:
        _local.in_log = False
