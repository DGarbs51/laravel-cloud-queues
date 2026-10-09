"""The threaded async adapters and the Backend's async factories."""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import Any

import anyio

from laravel_cloud_queues.transports import (
    AsyncConsumer,
    AsyncProducer,
    Backend,
    Delivery,
    OutgoingMessage,
    SentMessage,
)
from laravel_cloud_queues.transports._threaded import ThreadedConsumer, ThreadedProducer

DELIVERY = Delivery(message_id="m1", queue="q", body="{}", attempt=1, receipt="r")


class SyncProducer:
    max_payload_bytes = 42
    supports_fifo = True

    def __init__(self) -> None:
        self.threads: list[int] = []
        self.closed = False

    def send(self, message: OutgoingMessage) -> SentMessage:
        self.threads.append(threading.get_ident())
        return SentMessage(message_id="id-1", queue=message.queue)

    def close(self) -> None:
        self.closed = True


class SyncConsumer:
    supports_renewal = True

    def __init__(self) -> None:
        self.calls: list[tuple[str, object, int]] = []
        self.interrupted = 0
        self.leases: list[int] = []

    def _call(self, name: str, value: object) -> None:
        self.calls.append((name, value, threading.get_ident()))

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        self._call("receive", (tuple(queues), wait_seconds))
        return DELIVERY

    def complete(self, delivery: Delivery) -> None:
        self._call("complete", delivery.message_id)

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        self._call("release", (delivery.message_id, delay_seconds))

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        self._call("renew", (delivery.message_id, lease_seconds))

    def interrupt(self) -> None:
        self.interrupted += 1

    def close(self) -> None:
        self._call("close", None)


def test_threaded_producer_sends_in_a_worker_thread_and_leaves_the_producer_open() -> None:
    sync = SyncProducer()
    producer = ThreadedProducer(sync)
    assert isinstance(producer, AsyncProducer)
    assert (producer.max_payload_bytes, producer.supports_fifo) == (42, True)

    async def main() -> SentMessage:
        sent = await producer.send(OutgoingMessage(body="{}", queue="q"))
        await producer.aclose()
        return sent

    assert anyio.run(main) == SentMessage(message_id="id-1", queue="q")
    assert sync.threads[0] != threading.get_ident()
    assert not sync.closed


def test_threaded_consumer_runs_every_call_in_a_worker_thread() -> None:
    sync = SyncConsumer()
    consumer = ThreadedConsumer(sync)
    assert isinstance(consumer, AsyncConsumer)
    assert consumer.supports_renewal
    assert consumer.blocking is sync

    async def main() -> Delivery | None:
        received = await consumer.receive(["a", "b"], 2.5)
        await consumer.complete(DELIVERY)
        await consumer.release(DELIVERY, 7)
        await consumer.renew(DELIVERY, 60)
        await consumer.aclose()
        return received

    assert anyio.run(main) is DELIVERY
    assert [(name, value) for name, value, _ in sync.calls] == [
        ("receive", (("a", "b"), 2.5)),
        ("complete", "m1"),
        ("release", ("m1", 7)),
        ("renew", ("m1", 60)),
        ("close", None),
    ]
    assert threading.get_ident() not in {thread for _, _, thread in sync.calls}
    consumer.interrupt()
    assert sync.interrupted == 1


def _sync_backend(consumer: SyncConsumer, producer: SyncProducer, **factories: Any) -> Backend:
    def consumer_factory(*, lease_seconds: int = 60) -> SyncConsumer:
        consumer.leases.append(lease_seconds)
        return consumer

    return Backend("sqs", producer, consumer_factory, **factories)


def test_backend_falls_back_to_threaded_adapters() -> None:
    consumer, producer = SyncConsumer(), SyncProducer()
    backend = _sync_backend(consumer, producer)
    async_producer = backend.open_async_producer()
    assert isinstance(async_producer, ThreadedProducer)
    assert async_producer.max_payload_bytes == 42
    async_consumer = backend.open_async_consumer(lease_seconds=9)
    assert isinstance(async_consumer, ThreadedConsumer)
    assert async_consumer.blocking is consumer
    assert consumer.leases == [9]


def test_backend_uses_native_async_factories() -> None:
    native_producer, native_consumer = object(), object()
    leases: list[int] = []

    def async_consumer_factory(*, lease_seconds: int = 60) -> Any:
        leases.append(lease_seconds)
        return native_consumer

    backend = _sync_backend(
        SyncConsumer(),
        SyncProducer(),
        async_producer_factory=lambda: native_producer,
        async_consumer_factory=async_consumer_factory,
    )
    assert backend.open_async_producer() is native_producer
    assert backend.open_async_consumer(lease_seconds=5) is native_consumer
    assert backend.open_async_consumer() is native_consumer
    assert leases == [5, 60]
