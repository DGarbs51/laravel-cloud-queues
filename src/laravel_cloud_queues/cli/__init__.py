"""``laravel-cloud-queues`` console entry point (PROJECT_SCOPE.md §23). CONTRACT — lane L6.
See docs/contract/cli.md."""

from __future__ import annotations

from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """``work TARGET``, ``inspect TARGET``, ``conformance ...``. Returns the exit code."""
    raise NotImplementedError


__all__ = ["main"]
