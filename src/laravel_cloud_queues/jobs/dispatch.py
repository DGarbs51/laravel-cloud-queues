"""The single pipeline through which every job is dispatched.

``Job.dispatch`` and ``Job.dispatch_async`` both call :func:`prepare_dispatch` and then
:func:`send_prepared` (the async variant runs them in a worker thread), so validation,
serialization, routing, tracing and transport behavior are identical.

A dispatch runs through these steps in order:

1. Resolve the queue from the options, then the job, then the configured default.
2. Validate the options against the queue kind and backend. On ``.fifo`` queues the group
   defaults to the full queue name and the deduplication ID defaults to a new unique ID
   chosen once here, while ``""`` omits it. A positive delay on a FIFO queue, FIFO options on
   a standard queue, a fair-queue group on a FIFO queue, and any FIFO or fair-queue option on
   the redis backend are all rejected. Group and deduplication IDs must be 1-128 characters
   of the set SQS allows.
3. Encode the arguments and build the envelope with a new UUID, the policy, the queue, the
   dispatch time and the trace context.
4. Measure the UTF-8 body against ``producer.max_payload_bytes``, or the ``MAX_BODY_BYTES``
   decode ceiling, raising a :class:`PayloadTooLargeError` when it is exceeded.
5. Send the message, emit the ``queued`` event on a best-effort basis (managed mode only),
   and return a :class:`DispatchReceipt`.
"""

from __future__ import annotations

import logging
import re
import uuid as uuidlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from ..errors import InvalidQueueOptionError, PayloadTooLargeError
from ..observability import inject_trace_context, lifecycle_event
from ..transports.base import OutgoingMessage
from .envelope import MAX_BODY_BYTES, Envelope, encode_envelope
from .job import AnyJob, DispatchOptions, DispatchReceipt
from .policy import normalize_delay
from .signature import encode_arguments

logger = logging.getLogger(__name__)
"""The logger for the dispatch pipeline."""

_SQS_ID = re.compile(r"[!-~]{1,128}")
"""The pattern for SQS group and deduplication IDs.

SQS allows 1-128 alphanumeric or punctuation characters, which is printable ASCII without
the space.
"""


@dataclass(frozen=True)
class PreparedDispatch:
    """A validated and encoded dispatch that is ready to be sent."""

    job_name: str
    """The name of the job being dispatched."""
    uuid: str
    """The UUID assigned to the envelope."""
    message: OutgoingMessage
    """The message to hand to the producer."""


def prepare_dispatch(
    job: AnyJob,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    options: DispatchOptions,
) -> PreparedDispatch:
    """Prepare the given job and arguments for dispatch.

    This performs no I/O. Invalid options, arguments or payloads raise a subclass of
    :class:`DispatchError`.
    """
    registry = job.registry
    queue = options.queue if options.queue is not None else job.queue
    if queue is None:
        queue = registry.default_queue()
    if not queue:
        raise InvalidQueueOptionError("Queue name must be a non-empty string.")
    delay = 0 if options.delay is None else normalize_delay(options.delay)

    supports_fifo, max_payload_bytes = registry.producer_capabilities()
    fifo = supports_fifo and queue.endswith(".fifo")
    if fifo and delay > 0:
        raise InvalidQueueOptionError(
            f"FIFO queue [{queue}] does not support per-message delays (got {delay} seconds)."
        )
    group, deduplication_id, message_group = _message_group_options(
        queue, options, supports_fifo=supports_fifo, fifo=fifo
    )

    args_json, kwargs_json = encode_arguments(job.signature, registry.codecs, args, kwargs)
    message_uuid = str(uuidlib.uuid4())
    body = encode_envelope(
        Envelope(
            uuid=message_uuid,
            display_name=job.name,
            job=job.name,
            args=args_json,
            kwargs=kwargs_json,
            policy=job.policy,
            queue=queue,
            dispatched_at=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            context=inject_trace_context(),
        )
    )
    # Transports without a limit (redis) still get the decode ceiling, so every accepted
    # dispatch can be decoded by the worker.
    limit = MAX_BODY_BYTES if max_payload_bytes is None else max_payload_bytes
    size = len(body.encode("utf-8"))
    if size > limit:
        raise PayloadTooLargeError(size=size, limit=limit, queue=queue)
    return PreparedDispatch(
        job_name=job.name,
        uuid=message_uuid,
        message=OutgoingMessage(
            body=body,
            queue=queue,
            delay_seconds=delay,
            fifo_group=group,
            deduplication_id=deduplication_id,
            message_group=message_group,
        ),
    )


def _message_group_options(
    queue: str, options: DispatchOptions, *, supports_fifo: bool, fifo: bool
) -> tuple[str | None, str | None, str | None]:
    """Resolve the FIFO group, deduplication ID and fair-queue group for the message.

    The options are validated and defaulted, then returned as a
    ``(fifo_group, deduplication_id, message_group)`` tuple.
    """
    if not supports_fifo:
        if (options.group, options.deduplication_id, options.message_group) != (None, None, None):
            raise InvalidQueueOptionError(
                "FIFO and fair-queue options (group, deduplication_id, message_group) are not "
                "supported by the redis backend."
            )
        return None, None, None
    if fifo:
        if options.message_group is not None:
            raise InvalidQueueOptionError(
                f"message_group is a fair-queue option for standard queues; FIFO queue "
                f"[{queue}] takes group= instead."
            )
        # Laravel SqsQueue::getQueueableOptions: default group is the queue name (serializes
        # the whole queue); default dedup ID is new per logical dispatch; "" omits it.
        group = queue if options.group is None else options.group
        if options.deduplication_id is None:
            deduplication_id: str | None = str(uuidlib.uuid4())
        else:
            deduplication_id = options.deduplication_id or None
        _check_sqs_id("group", group)
        if deduplication_id is not None:
            _check_sqs_id("deduplication_id", deduplication_id)
        return group, deduplication_id, None
    if options.group is not None or options.deduplication_id is not None:
        raise InvalidQueueOptionError(
            f"group and deduplication_id are FIFO options; standard queue [{queue}] supports "
            "message_group for fair queues."
        )
    if options.message_group is not None:
        _check_sqs_id("message_group", options.message_group)
    return None, None, options.message_group


def _check_sqs_id(option: str, value: object) -> None:
    """Ensure the given value is a valid SQS group or deduplication ID."""
    if not isinstance(value, str) or not _SQS_ID.fullmatch(value):
        raise InvalidQueueOptionError(
            f"{option} must be 1-128 printable ASCII characters without spaces, got {value!r}."
        )


def send_prepared(job: AnyJob, prepared: PreparedDispatch) -> DispatchReceipt:
    """Send the prepared dispatch to the queue.

    This blocks while the producer sends the message and the ``queued`` event is emitted.
    It raises subclasses of :class:`DispatchError` and :class:`TransportError`.
    """
    registry = job.registry
    sent = registry.backend.producer.send(prepared.message)
    try:
        registry.telemetry.emit(
            lifecycle_event("queued", sent.queue, timestamp=datetime.now(timezone.utc))
        )
    except Exception:
        # The message is already sent: a telemetry failure must not look like a failed
        # dispatch (callers would retry and duplicate the job).
        logger.warning("Could not emit the queued event for job %s.", job.name, exc_info=True)
    return DispatchReceipt(message_id=sent.message_id, queue=sent.queue, uuid=prepared.uuid)
