"""The contract shared by every queue transport.

Transports are synchronous and body-opaque: they move UTF-8 ``str`` bodies and know
nothing about envelopes, jobs or events. Async callers offload them with
``anyio.to_thread.run_sync``. The worker's renewal watchdog calls :meth:`Consumer.renew`
from its own thread, so implementations must make ``renew`` thread-safe, although
nothing else runs concurrently with it on the same delivery.

Producers and consumers are separate because managed mode sends through SQS while
receiving through the in-container agent.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, TypeGuard, runtime_checkable

SQS_MAX_PAYLOAD_BYTES = 1_048_576
"""The maximum job payload size in bytes.

This matches Laravel's ``SqsQueue::MAX_SQS_PAYLOAD_SIZE`` and Laravel Cloud's documented
job payload limit.
"""

MAX_FRESH_DELAY_SECONDS = 900
"""The maximum number of seconds a newly sent message may be delayed."""

MAX_VISIBILITY_SECONDS = 43_200
"""The maximum number of seconds a received message may be hidden or released for."""


@dataclass(frozen=True)
class OutgoingMessage:
    """A validated, fully encoded message ready to be sent.

    Option validation happens in core before the message reaches the transport.
    """

    body: str
    """The encoded message body."""
    queue: str
    """The logical name of the queue, such as ``emails`` or ``orders.fifo``.

    The transport builds any URLs or keys from this name.
    """
    delay_seconds: int = 0
    """The number of whole seconds to delay the message, from 0 to 900.

    Core has already validated this value and rounded it up.
    """
    fifo_group: str | None = None
    """The ``MessageGroupId`` for ``.fifo`` queues.

    Core applies the default, which is the queue name.
    """
    deduplication_id: str | None = None
    """The ``MessageDeduplicationId`` for the message, chosen once.

    When ``None`` the identifier is omitted and content-based deduplication applies.
    """
    message_group: str | None = None
    """The fair-queue ``MessageGroupId`` on standard queues."""


@dataclass(frozen=True)
class SentMessage:
    """The result of successfully sending a message."""

    message_id: str
    """The identifier assigned to the message by the transport."""
    queue: str
    """The logical name of the queue the message was sent to."""


@dataclass(frozen=True)
class Delivery:
    """A single message received from the queue.

    The transport identity is captured before any decoding takes place.
    """

    message_id: str
    """The message identifier, stable across redeliveries.

    This is the SQS ``MessageId``, the agent ``messageId`` or the Redis job id.
    """
    queue: str
    """The normalized logical queue name, with any prefix and suffix rules reversed."""
    body: str
    """The raw message body."""
    attempt: int
    """The number of times the message has been received, counting from 1.

    This comes from ``ApproximateReceiveCount`` or the reservation counter, and a
    missing count is treated as 1.
    """
    receipt: str | None = field(default=None, repr=False)
    """The opaque completion token, such as a receipt handle or reserved member.

    This value is never logged.
    """
    received_at: float = 0.0
    """The ``time.monotonic()`` value at the moment the message was received."""
    meta: Mapping[str, str] = field(default_factory=dict[str, str], repr=False)
    """Additional transport details, such as ``queue_url``.

    These values are not logged by default.
    """


def json_object(value: object) -> TypeGuard[Mapping[str, object]]:
    """Determine if a decoded JSON value is an object, whose keys are always strings.

    The transports use this to narrow broker and agent payloads without ``Any``.
    """
    return isinstance(value, dict)


@runtime_checkable
class Producer(Protocol):
    """A transport that sends messages to the queue.

    One instance is shared per process, and it must be thread-safe.
    """

    @property
    def max_payload_bytes(self) -> int | None:
        """Get the UTF-8 byte limit for the encoded body.

        This is 1 MiB for SQS and ``None`` (no limit) for Redis.
        """
        ...

    @property
    def supports_fifo(self) -> bool:
        """Determine if the transport supports FIFO and fair-queue options.

        Redis returns ``False``, so those options are rejected.
        """
        ...

    def send(self, message: OutgoingMessage) -> SentMessage:
        """Send the message to the queue once.

        Retries inside the SDK reuse the same deduplication id. Raises a
        ``ManagedQueueNotFoundError``, ``TransportError`` or ``ConfigurationError``, or a
        ``PayloadTooLargeError`` when the queue's ``MaximumMessageSize`` rejects the body.
        """
        ...

    def close(self) -> None:
        """Close the producer and release its resources."""
        ...


@runtime_checkable
class Consumer(Protocol):
    """A transport that receives and settles deliveries for one worker process.

    Only one delivery is in flight at a time.
    """

    @property
    def supports_renewal(self) -> bool:
        """Determine if the watchdog should renew the lease on deliveries.

        This is ``True`` for direct SQS and Redis, and ``False`` for the agent.
        """
        ...

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Receive the next delivery, or ``None`` after roughly ``wait_seconds``.

        The queues are listed by priority, so the first queue with work wins. The agent
        consumer ignores them, since its assignment is authoritative and the worker checks
        for conflicts at startup. A ``TransportError`` is transient (the worker sleeps for a
        second and retries), while ``AgentUnavailableError`` and ``BrokerConnectionError``
        are fatal.
        """
        ...

    def complete(self, delivery: Delivery) -> None:
        """Complete the delivery after success or terminal failure.

        This deletes the SQS message, reports ``processed`` to the agent, or removes the
        reserved Redis member. Raises an ``AgentProtocolError`` when the agent rejects the
        request with a 4xx (nothing was applied and it must not be retried), or an
        ``AgentUnavailableError``, ``AmbiguousAcknowledgementError`` or ``LeaseLostError``.
        """
        ...

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Release the same message back onto the queue after the given delay.

        The delay is in whole seconds, from 0 to 43,200. This changes the SQS message
        visibility, reports ``released`` with a ``delay`` to the agent, or moves the Redis
        job from reserved to delayed. It never creates a new message, and it raises the
        same errors as :meth:`complete`.
        """
        ...

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Extend the delivery's visibility or reservation to now plus ``lease_seconds``.

        This is called from the watchdog thread. Raises a ``LeaseLostError`` if the worker
        no longer owns the message.
        """
        ...

    def interrupt(self) -> None:
        """Make a blocking :meth:`receive` return early during shutdown.

        This is best-effort and thread-safe.
        """
        ...

    def close(self) -> None:
        """Close the consumer and release its resources."""
        ...
