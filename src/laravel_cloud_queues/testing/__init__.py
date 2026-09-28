"""Testing primitives for application code (PROJECT_SCOPE.md §20). CONTRACT — lane L3c.

Eager mode exercises argument binding, payload validation, serialization/deserialization,
registration and (with the FastAPI invoker) DI + cleanup through the worker's execution
path. It does NOT replace transport or conformance testing.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from ..codecs import JSONValue


@dataclass(frozen=True)
class RecordedDispatch:
    job_name: str
    queue: str
    body: str
    args: tuple[JSONValue, ...]
    kwargs: Mapping[str, JSONValue]
    delay_seconds: int = 0
    fifo_group: str | None = None
    deduplication_id: str | None = None
    message_group: str | None = None


@dataclass
class DispatchRecorder:
    dispatched: list[RecordedDispatch] = field(default_factory=list)

    def for_job(self, job_name: str) -> list[RecordedDispatch]:
        return [d for d in self.dispatched if d.job_name == job_name]


__all__ = ["DispatchRecorder", "RecordedDispatch"]
