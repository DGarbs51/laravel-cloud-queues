"""Re-entry guard for observability logging.

A logging handler that writes back into the socket or stdout must not recurse, and
must not deadlock on the sink lock held by the emit that is logging the failure.
"""

from __future__ import annotations

import logging
import os
import select
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

_logger = logging.getLogger("laravel_cloud_queues.observability")
_local = threading.local()


def raw_write(fd: int, data: bytes, *, timeout: float = 0) -> None:
    """Best-effort alarm output; diagnostics write once, records get a bounded drain."""
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


def raw_diagnostic(message: str) -> None:
    """Best-effort alarm diagnostic: one write, no Python logging or stream locks."""
    raw_write(2, (message + "\n").encode("utf-8", errors="replace"))


@contextmanager
def signal_safe() -> Iterator[None]:
    """Keep telemetry fallback diagnostics off logging locks while handling SIGALRM."""
    previous = getattr(_local, "signal_safe", False)
    _local.signal_safe = True
    try:
        yield
    finally:
        _local.signal_safe = previous


def begin_call() -> bool:
    """Mark this thread as inside an observability call.

    Returns False when the thread is already inside one (a logging handler
    re-entered the sink). Callers must pair a True result with :func:`end_call`.
    """

    if getattr(_local, "depth", 0):
        return False
    _local.depth = 1
    return True


def end_call() -> None:
    """Clear the re-entry mark for this thread."""

    _local.depth = 0


def log_failure(message: str) -> None:
    """Log ``message`` locally. Never includes payloads, and never raises."""

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
