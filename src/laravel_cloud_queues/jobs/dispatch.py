"""The single dispatch pipeline (PROJECT_SCOPE.md §9, §10). CONTRACT — lane L3c.

``Job.dispatch`` and ``Job.dispatch_async`` both call :func:`prepare_dispatch` then
:func:`send_prepared` (async: inside ``anyio.to_thread.run_sync``), so validation,
serialization, routing, tracing and transport semantics are identical.

Steps: resolve queue (options > job > config default) -> validate options against queue
kind and backend (``.fifo``: group default = queue name incl. ``.fifo``, dedup default = new
unique ID chosen once here, ``""`` = omit; positive delay on FIFO rejected; FIFO options on
standard queues and fair groups on FIFO queues rejected; any FIFO/fair option in redis mode
rejected; group/dedup IDs 1-128 chars of SQS's allowed set) -> encode arguments -> build
envelope (new uuid, policy, queue, dispatched_at, trace context) -> measure UTF-8 bytes vs
``producer.max_payload_bytes`` (PayloadTooLargeError) -> ``producer.send`` -> emit ``queued``
(managed mode only, best-effort) -> DispatchReceipt.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ..transports.base import OutgoingMessage
from .job import AnyJob, DispatchOptions, DispatchReceipt


@dataclass(frozen=True)
class PreparedDispatch:
    job_name: str
    uuid: str
    message: OutgoingMessage


def prepare_dispatch(
    job: AnyJob,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    options: DispatchOptions,
) -> PreparedDispatch:
    """Pure CPU work, no I/O. Raises DispatchError subclasses."""
    raise NotImplementedError


def send_prepared(job: AnyJob, prepared: PreparedDispatch) -> DispatchReceipt:
    """Blocking: producer send + ``queued`` telemetry. Raises DispatchError subclasses and
    TransportError."""
    raise NotImplementedError
