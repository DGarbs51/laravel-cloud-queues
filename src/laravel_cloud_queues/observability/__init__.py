"""Laravel Cloud observability (PROJECT_SCOPE.md §15, D1, D12) and tracing (§16).

Events go to the log socket only in managed mode (D12). In ``sqs`` and ``redis`` modes
the worker writes structured JSON lines to its own stdout instead (D6b terminal
failures). Telemetry never raises to the caller and never converts a successful job
into a failure.
"""

from __future__ import annotations

from ._events import (
    EXCEPTION_PREVIEW_LIMIT,
    FAILED_JOB_LINE_LIMIT,
    LifecycleType,
    encode_event_line,
    failed_job_event,
    failure_log_record,
    format_timestamp,
    lifecycle_event,
    uuid7,
)
from ._socket import SocketEventSink
from ._telemetry import EventSink, NullSink, Telemetry
from ._tracing import activate_trace_context, inject_trace_context

__all__ = [
    "EXCEPTION_PREVIEW_LIMIT",
    "FAILED_JOB_LINE_LIMIT",
    "EventSink",
    "LifecycleType",
    "NullSink",
    "SocketEventSink",
    "Telemetry",
    "activate_trace_context",
    "encode_event_line",
    "failed_job_event",
    "failure_log_record",
    "format_timestamp",
    "inject_trace_context",
    "lifecycle_event",
    "uuid7",
]
