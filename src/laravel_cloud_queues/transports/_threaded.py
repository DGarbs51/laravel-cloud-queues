"""The async adapters that run a synchronous transport in a worker thread.

Backends without a native asyncio client (SQS, custom transports) are offered to async
callers through these, so the worker and ``dispatch_async`` have a single async code path.
"""

from __future__ import annotations

from collections.abc import Sequence

import anyio.to_thread

from .base import Consumer, Delivery, OutgoingMessage, Producer, SentMessage


class ThreadedProducer:
    """An async producer that sends through a sync producer in a worker thread.

    The sync producer belongs to the backend, so closing this adapter leaves it open.
    """

    def __init__(self, producer: Producer) -> None:
        """Create a new threaded producer instance."""
        self._producer = producer

    @property
    def max_payload_bytes(self) -> int | None:
        """Get the UTF-8 byte limit for the encoded body."""
        return self._producer.max_payload_bytes

    @property
    def supports_fifo(self) -> bool:
        """Determine if the transport supports FIFO and fair-queue options."""
        return self._producer.supports_fifo

    async def send(self, message: OutgoingMessage) -> SentMessage:
        """Send the message in a worker thread."""
        return await anyio.to_thread.run_sync(self._producer.send, message)

    async def aclose(self) -> None:
        """Do nothing, since the backend owns the sync producer."""


class ThreadedConsumer:
    """An async consumer that runs a sync consumer in a worker thread."""

    def __init__(self, consumer: Consumer) -> None:
        """Create a new threaded consumer instance."""
        self._consumer = consumer

    @property
    def supports_renewal(self) -> bool:
        """Determine if the worker should renew the lease on deliveries."""
        return self._consumer.supports_renewal

    @property
    def blocking(self) -> Consumer:
        """Get the wrapped sync consumer."""
        return self._consumer

    async def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Receive the next delivery in a worker thread."""
        return await anyio.to_thread.run_sync(self._consumer.receive, queues, wait_seconds)

    async def complete(self, delivery: Delivery) -> None:
        """Complete the delivery in a worker thread."""
        await anyio.to_thread.run_sync(self._consumer.complete, delivery)

    async def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Release the delivery in a worker thread."""
        await anyio.to_thread.run_sync(self._consumer.release, delivery, delay_seconds)

    async def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Renew the delivery's lease in a worker thread."""
        await anyio.to_thread.run_sync(self._consumer.renew, delivery, lease_seconds)

    def interrupt(self) -> None:
        """Interrupt the sync consumer's pending receive."""
        self._consumer.interrupt()

    async def aclose(self) -> None:
        """Close the sync consumer in a worker thread."""
        await anyio.to_thread.run_sync(self._consumer.close)
