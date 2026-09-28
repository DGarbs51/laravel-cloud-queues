"""Transport contract.

Transports are synchronous and body-opaque: they move UTF-8 ``str`` bodies and know
nothing about envelopes, jobs or events. Async callers offload them with
``anyio.to_thread.run_sync``; the worker's renewal watchdog calls :meth:`Consumer.renew`
from its own thread, so implementations must be thread-safe for ``renew`` concurrent
with nothing else on the same delivery.

Producer and consumer are separate because managed mode sends through SQS while
receiving through the in-container agent.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

SQS_MAX_PAYLOAD_BYTES = 1_048_576
"""Laravel ``SqsQueue::MAX_SQS_PAYLOAD_SIZE``; Laravel Cloud's documented job payload limit."""

MAX_FRESH_DELAY_SECONDS = 900
MAX_VISIBILITY_SECONDS = 43_200


@dataclass(frozen=True)
class OutgoingMessage:
    """A validated, fully encoded message. Option validation happens in core before this."""

    body: str
    queue: str
    """Logical queue name (``emails``, ``orders.fifo``); the transport builds URLs/keys."""
    delay_seconds: int = 0
    """Whole seconds, 0..900, already validated and rounded up by core."""
    fifo_group: str | None = None
    """``MessageGroupId`` for ``.fifo`` queues (core applies the default: the queue name)."""
    deduplication_id: str | None = None
    """``MessageDeduplicationId``; ``None`` = omit (content-based dedup). Chosen once."""
    message_group: str | None = None
    """Fair-queue ``MessageGroupId`` on standard queues."""


@dataclass(frozen=True)
class SentMessage:
    message_id: str
    queue: str
    """Logical queue name."""


@dataclass(frozen=True)
class Delivery:
    """One received delivery. Transport identity is captured before any decoding."""

    message_id: str
    """Stable across redeliveries (SQS MessageId, agent messageId, Redis job id)."""
    queue: str
    """Normalized logical queue name (inverse of prefix/suffix rules)."""
    body: str
    attempt: int
    """``ApproximateReceiveCount`` / reservation counter; missing counts as 1."""
    receipt: str | None = field(default=None, repr=False)
    """Opaque completion token (receipt handle / reserved member). Never logged."""
    received_at: float = 0.0
    """``time.monotonic()`` at receipt."""
    meta: Mapping[str, str] = field(default_factory=dict, repr=False)
    """Transport extras (e.g. ``queue_url``). Never logged by default."""


@runtime_checkable
class Producer(Protocol):
    """Sends messages. One instance per process; thread-safe."""

    @property
    def max_payload_bytes(self) -> int | None:
        """UTF-8 byte limit for the encoded body (SQS 1 MiB; Redis ``None``)."""
        ...

    @property
    def supports_fifo(self) -> bool:
        """``False`` for Redis: FIFO and fair-queue options are rejected."""
        ...

    def send(self, message: OutgoingMessage) -> SentMessage:
        """Send once (SDK-internal retries reuse the same dedup ID).

        Raises ManagedQueueNotFoundError, PayloadTooLargeError (queue-level
        ``MaximumMessageSize`` rejection only), TransportError, ConfigurationError.
        """
        ...

    def close(self) -> None: ...


@runtime_checkable
class Consumer(Protocol):
    """Receives and settles deliveries for one worker process (one in flight)."""

    @property
    def supports_renewal(self) -> bool:
        """``True`` for direct SQS and Redis (watchdog renews); ``False`` for the agent."""
        ...

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Return the next delivery or ``None`` after at most ~``wait_seconds``.

        ``queues`` is a priority list (first queue with work wins). The agent consumer
        ignores it (its assignment is authoritative; the worker validates conflicts at
        startup). Raises TransportError (transient: worker sleeps 1 s and retries),
        AgentUnavailableError / BrokerConnectionError (fatal).
        """
        ...

    def complete(self, delivery: Delivery) -> None:
        """Success or terminal failure: DeleteMessage / ``processed`` / ZREM reserved.

        Raises AgentProtocolError (agent 4xx; not applied, do not retry),
        AgentUnavailableError, AmbiguousAcknowledgementError, LeaseLostError.
        """
        ...

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Retry the SAME message after ``delay_seconds`` (0..43,200, whole seconds).

        ChangeMessageVisibility / ``released`` with ``delay`` / reserved -> delayed.
        Never creates a new message. Same error contract as :meth:`complete`.
        """
        ...

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Extend visibility/reservation to now + ``lease_seconds``. Called from the watchdog
        thread. Raises LeaseLostError if the worker no longer owns the message."""
        ...

    def interrupt(self) -> None:
        """Best-effort: make a blocking :meth:`receive` return early (shutdown). Thread-safe."""
        ...

    def close(self) -> None: ...
