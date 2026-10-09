"""Exercise synchronous and native asyncio Redis transports against the same broker."""

from __future__ import annotations

import asyncio
import json
import time
from hashlib import sha256
from threading import Thread

import pytest
import redis

from laravel_cloud_queues.config import RedisConfig
from laravel_cloud_queues.errors import LeaseLostError
from laravel_cloud_queues.transports._threaded import ThreadedConsumer, ThreadedProducer
from laravel_cloud_queues.transports.base import OutgoingMessage
from laravel_cloud_queues.transports.redis import (
    AsyncRedisConsumer,
    AsyncRedisProducer,
    RedisConsumer,
    RedisProducer,
)

pytestmark = [pytest.mark.redis, pytest.mark.anyio]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=["threaded", "native"])
async def broker(request, redis_url, redis_prefix):
    config = RedisConfig(url=redis_url, prefix=redis_prefix)
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    sync = RedisProducer(config)
    producer = ThreadedProducer(sync) if request.param == "threaded" else AsyncRedisProducer(config)
    consumer = (
        ThreadedConsumer(RedisConsumer(config))
        if request.param == "threaded"
        else AsyncRedisConsumer(config)
    )
    try:
        yield config, client, producer, consumer
    finally:
        await consumer.aclose()
        await producer.aclose()
        sync.close()
        client.close()


async def test_dispatch_reserve_retry_and_complete(broker):
    config, client, producer, consumer = broker
    body = "opaque non-envelope \x00 Unicode ✓"
    sent = await producer.send(OutgoingMessage(body, "emails"))
    pending = f"{config.prefix}queues:emails"
    assert json.loads(client.lindex(pending, 0)) == {
        "id": sent.message_id,
        "attempts": 0,
        "body": body,
    }
    before = time.monotonic()
    delivery = await consumer.receive(["emails"], 0)
    assert (delivery.message_id, delivery.queue, delivery.body, delivery.attempt) == (
        sent.message_id,
        "emails",
        body,
        1,
    )
    assert before <= delivery.received_at <= time.monotonic()
    assert client.zrange(pending + ":reserved", 0, -1) == [delivery.receipt]
    await consumer.renew(delivery, 120)
    clock = client.time()
    now = clock[0] + clock[1] / 1_000_000
    assert now + 119 <= client.zscore(pending + ":reserved", delivery.receipt) <= now + 121
    await consumer.release(delivery, 0)
    second = await consumer.receive(["emails"], 0)
    assert second.message_id == delivery.message_id
    assert second.attempt == 2
    await consumer.complete(second)
    assert await consumer.receive(["emails"], 0) is None
    assert client.zcard(pending + ":reserved") == 0


async def test_delayed_job_becomes_visible_during_wait(broker):
    _, _, producer, consumer = broker
    start = time.monotonic()
    sent = await producer.send(OutgoingMessage("body", "delayed", delay_seconds=1))
    assert await consumer.receive(["delayed"], 0) is None
    delivery = await consumer.receive(["delayed"], 3)
    assert delivery.message_id == sent.message_id
    assert 0.9 <= time.monotonic() - start < 2.5
    await consumer.complete(delivery)


async def test_queue_priority(broker):
    _, _, producer, consumer = broker
    low = await producer.send(OutgoingMessage("low", "low"))
    high = await producer.send(OutgoingMessage("high", "high"))
    first = await consumer.receive(["high", "low"], 0)
    second = await consumer.receive(["high", "low"], 0)
    assert first.message_id == high.message_id
    assert second.message_id == low.message_id
    await consumer.complete(first)
    await consumer.complete(second)


@pytest.mark.parametrize("operation", ["complete", "release", "renew"])
@pytest.mark.parametrize("rereserve", [False, True])
async def test_expired_reservation_cannot_settle_or_revive(broker, operation, rereserve):
    config, client, producer, consumer = broker
    await producer.send(OutgoingMessage("body", "expiry"))
    stale = await consumer.receive(["expiry"], 0)
    reserved = f"{config.prefix}queues:expiry:reserved"
    client.zadd(reserved, {stale.receipt: 0})
    current = await consumer.receive(["expiry"], 0) if rereserve else None
    args = (stale,) if operation == "complete" else (stale, 60)
    with pytest.raises(LeaseLostError):
        await getattr(consumer, operation)(*args)
    assert client.zcard(f"{config.prefix}queues:expiry:delayed") == 0
    if rereserve:
        assert current.message_id == stale.message_id
        assert current.attempt == 2
        await consumer.complete(current)
    else:
        assert client.zscore(reserved, stale.receipt) == 0


@pytest.mark.parametrize("member", [b"invalid \xff", b"[]", b'{"id":"x","body":"b","attempts":-1}'])
async def test_malformed_wrapper_preserves_receipt_and_can_be_completed(broker, member):
    config, client, producer, consumer = broker
    pending = f"{config.prefix}queues:broken"
    client.rpush(pending, member)
    sent = await producer.send(OutgoingMessage("valid", "broken"))
    malformed = await consumer.receive(["broken"], 0)
    assert malformed.message_id == "malformed-" + sha256(member).hexdigest()
    assert malformed.receipt.encode("utf-8", errors="surrogateescape") == member
    assert malformed.attempt == 1
    await consumer.renew(malformed, 60)
    await consumer.release(malformed, 0)
    malformed = await consumer.receive(["broken"], 0)
    # The good job was already pending, so it wins over the migrated malformed job.
    assert malformed.message_id == sent.message_id
    await consumer.complete(malformed)
    malformed = await consumer.receive(["broken"], 0)
    await consumer.complete(malformed)
    assert client.zcard(pending + ":reserved") == 0


async def test_empty_wait_is_bounded(broker):
    _, _, _, consumer = broker
    start = time.monotonic()
    assert await consumer.receive(["empty"], 0.15) is None
    assert 0.1 <= time.monotonic() - start < 0.7


async def test_notification_wakes_wait(broker):
    _, _, producer, consumer = broker
    receive = asyncio.create_task(consumer.receive(["empty"], 10))
    await asyncio.sleep(0.1)
    start = time.monotonic()
    sent = await producer.send(OutgoingMessage("wake", "empty"))
    delivery = await asyncio.wait_for(receive, 0.5)
    assert time.monotonic() - start < 0.5
    assert delivery.message_id == sent.message_id
    await consumer.complete(delivery)


@pytest.mark.parametrize("async_producer", [False, True])
async def test_sync_async_interoperability(redis_url, redis_prefix, async_producer):
    config = RedisConfig(url=redis_url, prefix=redis_prefix)
    sync_producer, sync_consumer = RedisProducer(config), RedisConsumer(config)
    producer, consumer = AsyncRedisProducer(config), AsyncRedisConsumer(config)
    try:
        message = OutgoingMessage("interop", "default")
        if async_producer:
            sent = await producer.send(message)
            delivery = sync_consumer.receive(["default"], 0)
            sync_consumer.renew(delivery, 60)
            sync_consumer.release(delivery, 0)
            redelivered = await consumer.receive(["default"], 0)
            consumer.blocking.complete(redelivered)
        else:
            sent = sync_producer.send(message)
            delivery = await consumer.receive(["default"], 0)
            # The signal/watchdog twin accepts the exact same reservation.
            consumer.blocking.renew(delivery, 60)
            consumer.blocking.release(delivery, 0)
            redelivered = sync_consumer.receive(["default"], 0)
            sync_consumer.complete(redelivered)
        assert delivery.message_id == redelivered.message_id == sent.message_id
        assert delivery.body == redelivered.body == "interop"
        assert redelivered.attempt == 2
    finally:
        await producer.aclose()
        await consumer.aclose()
        sync_producer.close()
        sync_consumer.close()


@pytest.mark.parametrize("from_thread", [False, True])
async def test_interrupt_during_long_wait_returns_within_half_second(
    redis_url, redis_prefix, monkeypatch, from_thread
):
    consumer = AsyncRedisConsumer(RedisConfig(url=redis_url, prefix=redis_prefix))
    waiting = asyncio.Event()
    exchange = consumer._exchange

    async def observed(connection, args, reporting):
        if args[0] == "BLPOP":
            waiting.set()
        return await exchange(connection, args, reporting)

    monkeypatch.setattr(consumer, "_exchange", observed)
    try:
        receive = asyncio.create_task(consumer.receive(["empty"], 10))
        await asyncio.wait_for(waiting.wait(), 2)
        start = time.monotonic()
        if from_thread:
            thread = Thread(target=consumer.interrupt)
            thread.start()
            thread.join(timeout=0.1)
            assert not thread.is_alive()
        else:
            consumer.interrupt()
        assert await asyncio.wait_for(receive, 0.5) is None
        assert time.monotonic() - start < 0.5
        assert not consumer._pool._in_use_connections
        assert all(not conn.is_connected for conn in consumer._pool._available_connections)
    finally:
        await consumer.aclose()


async def test_native_operations_never_use_thread_offload(redis_url, redis_prefix, monkeypatch):
    config = RedisConfig(url=redis_url, prefix=redis_prefix)
    producer, consumer = AsyncRedisProducer(config), AsyncRedisConsumer(config)

    def forbidden(*args, **kwargs):
        raise AssertionError("Native Redis I/O must stay on the event loop")

    monkeypatch.setattr("anyio.to_thread.run_sync", forbidden)
    try:
        sent = await producer.send(OutgoingMessage("native", "default"))
        delivery = await consumer.receive(["default"], 0)
        assert delivery.message_id == sent.message_id
        await consumer.renew(delivery, 60)
        await consumer.release(delivery, 0)
        redelivered = await consumer.receive(["default"], 0)
        await consumer.complete(redelivered)
        assert await consumer.receive(["default"], 0) is None
    finally:
        await producer.aclose()
        await consumer.aclose()


async def test_close_interrupts_pending_notification_wait(redis_url, redis_prefix, monkeypatch):
    consumer = AsyncRedisConsumer(RedisConfig(url=redis_url, prefix=redis_prefix))
    waiting = asyncio.Event()
    exchange = consumer._exchange

    async def observed(connection, args, reporting):
        if args[0] == "BLPOP":
            waiting.set()
        return await exchange(connection, args, reporting)

    monkeypatch.setattr(consumer, "_exchange", observed)
    receive = asyncio.create_task(consumer.receive(["empty"], 10))
    try:
        await asyncio.wait_for(waiting.wait(), 2)
        await consumer.aclose()
        assert await asyncio.wait_for(receive, 0.5) is None
        assert not consumer._pool._in_use_connections
    finally:
        await consumer.aclose()
