"""Re-entry guard for observability logging.

A logging handler that writes back into the socket or stdout must not recurse, and
must not deadlock on the sink lock held by the emit that is logging the failure.
"""

from __future__ import annotations

import logging
import threading

_logger = logging.getLogger("laravel_cloud_queues.observability")
_local = threading.local()


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

    if getattr(_local, "in_log", False):
        return
    _local.in_log = True
    try:
        _logger.warning("%s", message)
    except Exception:
        return
    finally:
        _local.in_log = False
