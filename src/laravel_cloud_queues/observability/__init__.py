"""The Laravel Cloud observability and tracing layer.

Events are sent to the log socket only in managed mode. In ``sqs`` and ``redis`` modes
the worker writes terminal failures to its own stdout as structured JSON lines instead.
Telemetry never raises to the caller and never turns a successful job into a failure.
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
