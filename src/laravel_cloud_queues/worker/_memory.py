"""Resolve worker memory limits and measure resident memory."""

from __future__ import annotations

import math
import os
import resource
import sys
from pathlib import Path

from ..errors import ConfigurationError

_MIB = 1024**2


def resolve_limit(value: str | None, *, managed: bool) -> int | None:
    """Resolve the worker option, environment override or managed cgroup default in bytes.

    Zero disables recycling. An explicit percentage needs a finite cgroup v2 limit.
    Invalid settings raise :class:`ConfigurationError` before a consumer is opened.
    """
    if value is None:
        value = os.environ.get("LARAVEL_CLOUD_QUEUES_MEMORY")
    if value is None:
        limit = _cgroup_limit() if managed else None
        return limit * 90 // 100 if limit is not None else None
    value = value.strip()
    percent = value.endswith("%")
    try:
        number = float(value[:-1] if percent else value)
    except ValueError:
        raise ConfigurationError(
            "Memory must be a non-negative MiB value or a percentage."
        ) from None
    if not math.isfinite(number) or number < 0 or (percent and number > 100):
        raise ConfigurationError("Memory must be finite, non-negative, and at most 100%.")
    if number == 0:
        return None
    if percent:
        limit = _cgroup_limit()
        if limit is None:
            raise ConfigurationError(
                "Percentage memory needs a finite cgroup v2 memory.max limit; use MiB instead."
            )
        size = limit * (number / 100)
    else:
        size = number * _MIB
    if not math.isfinite(size):
        raise ConfigurationError("Memory limit is too large.")
    return max(1, int(size))


def _cgroup_limit() -> int | None:
    """Read the finite cgroup v2 memory limit, if available."""
    try:
        limit = int(Path("/sys/fs/cgroup/memory.max").read_text(encoding="ascii").strip())
    except (OSError, ValueError, UnicodeError):
        return None
    return limit if limit > 0 else None


def current_rss() -> int:
    """Measure current RSS on Linux, falling back to the platform's peak RSS in bytes."""
    if sys.platform.startswith("linux"):
        try:
            pages = int(Path("/proc/self/statm").read_text(encoding="ascii").split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError, IndexError, UnicodeError):
            pass
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)
