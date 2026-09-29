"""Laravel conformance references use framework v13.33.0, src/Illuminate/Queue/."""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from hashlib import sha256
from threading import Event
from uuid import UUID, uuid4

import pytest

from laravel_cloud_queues import Registry
from laravel_cloud_queues.config import QueueConfig, RedisConfig
from laravel_cloud_queues.errors import ConfigurationError, LeaseLostError
from laravel_cloud_queues.transports import Backend
from laravel_cloud_queues.transports.base import OutgoingMessage
from laravel_cloud_queues.transports.redis import RedisConsumer, RedisProducer

pytestmark = pytest.mark.redis


def unavailable(reason):
    if os.environ.get("LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES", "").lower() in {"1", "true", "yes"}:
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture
def broker(request):
    try:
        import redis
    except ImportError:
        unavailable("Redis integration tests require the redis extra")
    tls = getattr(request, "param", None) == "tls"
    url = os.environ.get(
        "LARAVEL_CLOUD_QUEUES_TEST_REDIS_TLS_URL" if tls else "LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL",
        "" if tls else "redis://127.0.0.1:6379/15",
    )
    if not url:
        unavailable("LARAVEL_CLOUD_QUEUES_TEST_REDIS_TLS_URL is not set")
    if tls:
        assert url.startswith("rediss://")
    config = RedisConfig(url=url, prefix=f"lcq-test:{uuid4()}:")
    client = redis.Redis.from_url(
        url, decode_responses=True, socket_connect_timeout=1, socket_timeout=2
    )
    try:
        client.ping()
    except redis.RedisError:
        client.close()
        unavailable("Redis/Valkey integration service unavailable")
    producer = RedisProducer(config)
    consumer = RedisConsumer(config)
    try:
        yield config, client, producer, consumer
    finally:
        producer.close()
        consumer.close()
        keys = list(client.scan_iter(match=f"{config.prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()


@pytest.mark.parametrize("suffix", ["delayed", "reserved", "notify"])
@pytest.mark.parametrize("delay", [0, 60])
def test_dispatch_cannot_alias_another_queues_internal_keys(broker, suffix, delay):
    config, client, producer, consumer = broker
    registry = Registry(
        config=QueueConfig(mode="redis", redis=config),
        backend=Backend(
            mode="redis", producer=producer, consumer_factory=partial(RedisConsumer, config)
        ),
    )
    job = registry.job(name="alias.test")(lambda: None)
    with pytest.raises(ConfigurationError, match="Redis queue names must not end with"):
        job.options(queue=f"orders:{suffix}", delay=delay).dispatch()
    assert list(client.scan_iter(match=f"{config.prefix}*")) == []

    sent = job.options(queue="orders").dispatch()
    delivery = consumer.receive(["orders"], 0)
    assert delivery is not None
    assert delivery.message_id == sent.message_id
    consumer.renew(delivery, 60)
    consumer.release(delivery, 0)
    redelivered = consumer.receive(["orders"], 0)
    assert redelivered is not None
    assert redelivered.message_id == sent.message_id
    assert redelivered.attempt == 2
    consumer.complete(redelivered)
    assert consumer.receive(["orders"], 0) is None


@pytest.mark.parametrize(
    "queue",
    [
        "reserved",
        "tenant:orders",
        "orders:reserved:child",
        "orders:notify-other",
        "orders:delayed:",
    ],
)
def test_non_aliasing_queue_names_keep_existing_keys(broker, queue):
    config, client, producer, consumer = broker
    sent = producer.send(OutgoingMessage("body", queue))
    assert client.llen(f"{config.prefix}queues:{queue}") == 1
    delivery = consumer.receive([queue], 0)
    assert delivery is not None
    assert (delivery.message_id, delivery.queue, delivery.body) == (sent.message_id, queue, "body")
    consumer.complete(delivery)


@pytest.mark.parametrize("terminal", [False, True])
def test_dispatch_reserve_complete(broker, terminal):
    """LuaScripts.php:35-38,97-106; RedisQueue.php:617-620: terminal also deletes."""
    config, client, producer, consumer = broker
    body = "opaque non-envelope \x00 Unicode ✓" if terminal else '{"uuid":"different"}'
    sent = producer.send(OutgoingMessage(body, "emails"))
    assert UUID(sent.message_id).version == 4
    pending = f"{config.prefix}queues:emails"
    wrapper = json.loads(client.lindex(pending, 0))
    assert wrapper == {"id": sent.message_id, "attempts": 0, "body": body}
    assert client.llen(pending + ":notify") == 1
    before = time.monotonic()
    delivery = consumer.receive(["emails"], 0)
    assert delivery is not None
    assert (delivery.message_id, delivery.queue, delivery.body, delivery.attempt) == (
        sent.message_id,
        "emails",
        body,
        1,
    )
    assert before <= delivery.received_at <= time.monotonic()
    assert client.zrange(pending + ":reserved", 0, -1) == [delivery.receipt]
    assert client.llen(pending) == client.llen(pending + ":notify") == 0
    consumer.complete(delivery)
    assert client.zcard(pending + ":reserved") == 0
    assert consumer.receive(["emails"], 0) is None


def test_retry_preserves_exact_member_id_and_counts(broker):
    """LuaScripts.php:123-132; Jobs/RedisJob.php:104-106: increment only on reserve."""
    config, client, producer, consumer = broker
    sent = producer.send(OutgoingMessage("body", "retry"))
    for attempt, delay in enumerate([1, 0, 0], start=1):
        delivery = consumer.receive(["retry"], 2)
        assert delivery is not None
        assert (delivery.message_id, delivery.attempt) == (sent.message_id, attempt)
        consumer.release(delivery, delay)
        assert client.zrange(f"{config.prefix}queues:retry:delayed", 0, -1) == [delivery.receipt]
        if delay:
            assert consumer.receive(["retry"], 0) is None
    last = consumer.receive(["retry"], 0)
    assert last is not None
    assert last.attempt == 4
    consumer.complete(last)


def test_delayed_job_becomes_visible_during_blocking_wait(broker):
    """LuaScripts.php:75-80,146-165: delayed jobs migrate with notification tokens."""
    _, _, producer, consumer = broker
    start = time.monotonic()
    sent = producer.send(OutgoingMessage("body", "delayed", delay_seconds=1))
    assert consumer.receive(["delayed"], 0) is None
    delivery = consumer.receive(["delayed"], 3)
    assert delivery is not None
    assert delivery.message_id == sent.message_id
    assert 0.9 <= time.monotonic() - start < 2.5
    consumer.complete(delivery)


def test_expiry_after_crash_redelivers(broker):
    """RedisQueue.php:559-565; LuaScripts.php:146-165: expired reservations migrate."""
    config, _, producer, consumer = broker
    crashed = RedisConsumer(config, lease_seconds=1)
    try:
        sent = producer.send(OutgoingMessage("body", "crash"))
        first = crashed.receive(["crash"], 0)
        assert first is not None
    finally:
        crashed.close()  # Simulated crash: no completion or release.
    delivery = consumer.receive(["crash"], 3)
    assert delivery is not None
    assert delivery.message_id == sent.message_id
    assert delivery.attempt == 2
    assert delivery.receipt != first.receipt
    consumer.complete(delivery)


def test_watchdog_renewal_prevents_redelivery_past_initial_lease(broker):
    """D7 / architecture decision 4: pooled renew works while the main thread is busy."""
    config, _, producer, competitor = broker
    consumer = RedisConsumer(config, lease_seconds=1)
    stop = Event()
    producer.send(OutgoingMessage("body", "long"))
    delivery = consumer.receive(["long"], 0)
    assert delivery is not None

    def watchdog():
        while not stop.wait(0.2):
            consumer.renew(delivery, 1)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(watchdog)
            try:
                assert competitor.receive(["long"], 2.1) is None
            finally:
                stop.set()
                future.result()
        consumer.complete(delivery)
    finally:
        consumer.close()


@pytest.mark.parametrize("operation", ["complete", "release", "renew"])
@pytest.mark.parametrize("rereserve", [False, True])
def test_expired_receipt_cannot_settle_or_revive(broker, operation, rereserve):
    """D7 lost-lease guard strengthens RedisQueue.php:617-636 (unguarded in Laravel)."""
    config, client, producer, consumer = broker
    producer.send(OutgoingMessage("body", "expiry"))
    stale = consumer.receive(["expiry"], 0)
    assert stale is not None
    reserved = f"{config.prefix}queues:expiry:reserved"
    client.zadd(reserved, {stale.receipt: 0})
    current = consumer.receive(["expiry"], 0) if rereserve else None
    args = (stale,) if operation == "complete" else (stale, 60)
    with pytest.raises(LeaseLostError):
        getattr(consumer, operation)(*args)
    assert client.zcard(f"{config.prefix}queues:expiry:delayed") == 0
    assert client.zcard(reserved) == 1
    if rereserve:
        assert current is not None
        assert current.attempt == 2
        assert client.zscore(reserved, current.receipt) is not None
        consumer.complete(current)
    else:
        assert client.zscore(reserved, stale.receipt) == 0


def test_concurrent_reservations_are_unique(broker):
    """LuaScripts.php:97-106: atomic pop/increment/reserve across eight workers."""
    config, client, producer, _ = broker
    ids = {
        producer.send(OutgoingMessage(str(index), "concurrent")).message_id for index in range(500)
    }

    def drain():
        consumer = RedisConsumer(config)
        deliveries = []
        try:
            while (delivery := consumer.receive(["concurrent"], 0)) is not None:
                deliveries.append(delivery)
            return deliveries
        finally:
            consumer.close()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(drain) for _ in range(8)]
        deliveries = [delivery for future in futures for delivery in future.result()]
    assert len(deliveries) == 500
    assert {delivery.message_id for delivery in deliveries} == ids
    assert all(delivery.attempt == 1 for delivery in deliveries)
    assert client.zcard(f"{config.prefix}queues:concurrent:reserved") == 500


def test_priority_order(broker):
    """RedisQueue.php:529-549: priority follows the worker's ordered queue list."""
    _, _, producer, consumer = broker
    low = producer.send(OutgoingMessage("low", "low"))
    high = producer.send(OutgoingMessage("high", "high"))
    first = consumer.receive(["high", "low"], 0)
    second = consumer.receive(["high", "low"], 0)
    assert first is not None
    assert first.message_id == high.message_id
    assert second is not None
    assert second.message_id == low.message_id
    consumer.complete(first)
    consumer.complete(second)


def test_empty_wait_is_bounded_and_uses_blpop(broker, monkeypatch):
    """RedisQueue.php:602-604: block on notify, with bounded fractional timeout here."""
    _, _, _, consumer = broker
    command = consumer._command
    waits = []

    def observed(*args, **kwargs):
        if args[0] == "BLPOP":
            waits.append(args[-1])
        return command(*args, **kwargs)

    monkeypatch.setattr(consumer, "_command", observed)
    start = time.monotonic()
    assert consumer.receive(["empty"], 0.25) is None
    assert 0.2 <= time.monotonic() - start < 0.75
    assert len(waits) == 1
    assert 0 < waits[0] <= 0.25


@pytest.mark.parametrize("interrupt", [False, True])
def test_push_wakes_blocking_wait_and_interrupt_is_prompt(broker, monkeypatch, interrupt):
    """LuaScripts.php:35-38; RedisQueue.php:602-604: notifications wake receivers."""
    _, _, producer, consumer = broker
    blocking = Event()
    command = consumer._command

    def observed(*args, **kwargs):
        if args[0] == "BLPOP":
            blocking.set()
        return command(*args, **kwargs)

    monkeypatch.setattr(consumer, "_command", observed)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(consumer.receive, ["high", "low"], 20)
        assert blocking.wait(2)
        start = time.monotonic()
        if interrupt:
            consumer.interrupt()
        else:
            sent = producer.send(OutgoingMessage("wake", "low"))
        delivery = future.result(timeout=2)
    assert time.monotonic() - start < (1.5 if interrupt else 0.75)
    if interrupt:
        assert delivery is None
    else:
        assert delivery is not None
        assert delivery.message_id == sent.message_id
        consumer.complete(delivery)


def test_server_time_sets_all_scores(broker, monkeypatch):
    """D13 lease decision; TIME + effects replication is supported on Valkey 8/Redis 7+."""
    config, client, producer, consumer = broker
    monkeypatch.setattr(time, "time", lambda: 1)
    producer.send(OutgoingMessage("body", "clock", delay_seconds=900))
    delayed = f"{config.prefix}queues:clock:delayed"
    clock = client.time()
    now = clock[0] + clock[1] / 1_000_000
    member, score = client.zrange(delayed, 0, -1, withscores=True)[0]
    assert now + 899 <= score <= now + 901
    client.zadd(delayed, {member: 0})
    delivery = consumer.receive(["clock"], 0)
    assert delivery is not None
    reserved = f"{config.prefix}queues:clock:reserved"
    assert now + 59 <= client.zscore(reserved, delivery.receipt) <= now + 61
    consumer.renew(delivery, 100)
    assert now + 99 <= client.zscore(reserved, delivery.receipt) <= now + 101
    consumer.release(delivery, 50)
    assert now + 49 <= client.zscore(delayed, delivery.receipt) <= now + 51


@pytest.mark.parametrize("wrapper", ["invalid ✓", '{"id":"a","body":"b","attempts":-1}', "[]", ""])
def test_invalid_transport_wrapper_can_be_completed_without_blocking_queue(broker, wrapper):
    """Malformed entries reach the worker's terminal-failure path, then are deleted."""
    config, client, producer, consumer = broker
    pending = f"{config.prefix}queues:broken"
    client.rpush(pending, wrapper)
    client.rpush(pending + ":notify", 1)
    sent = producer.send(OutgoingMessage("valid body", "broken"))

    malformed = consumer.receive(["broken"], 0)
    assert malformed is not None
    assert malformed.message_id == "malformed-" + sha256(wrapper.encode("utf-8")).hexdigest()
    assert malformed.queue == "broken"
    assert malformed.body == malformed.receipt == wrapper
    assert malformed.attempt == 1
    assert client.zrange(pending + ":reserved", 0, -1) == [wrapper]
    assert client.llen(pending) == client.llen(pending + ":notify") == 1
    consumer.complete(malformed)
    assert client.zcard(pending + ":reserved") == 0

    valid = consumer.receive(["broken"], 0)
    assert valid is not None
    assert valid.message_id == sent.message_id
    assert valid.body == "valid body"
    assert valid.attempt == 1
    consumer.complete(valid)
    assert consumer.receive(["broken"], 0) is None


def test_no_package_payload_limit(broker):
    _, _, producer, consumer = broker
    body = "x" * 1_048_577
    producer.send(OutgoingMessage(body, "large"))
    delivery = consumer.receive(["large"], 0)
    assert delivery is not None
    assert delivery.body == body
    consumer.complete(delivery)


@pytest.mark.parametrize("broker", ["tls"], indirect=True)
def test_tls_roundtrip(broker):
    _, _, producer, consumer = broker
    sent = producer.send(OutgoingMessage("TLS", "tls"))
    delivery = consumer.receive(["tls"], 1)
    assert delivery is not None
    assert delivery.message_id == sent.message_id
    consumer.complete(delivery)


@pytest.mark.parametrize("attempts", [10**14 - 1, 10**14, 123456789012345])
def test_attempts_cjson_precision_boundary(broker, attempts):
    config, client, _, consumer = broker
    member = json.dumps({"id": "boundary", "body": "opaque", "attempts": attempts})
    client.rpush(f"{config.prefix}queues:precision", member)
    delivery = consumer.receive(["precision"], 0)
    assert delivery is not None
    assert type(delivery.attempt) is int
    if attempts >= 10**14:
        assert delivery.message_id.startswith("malformed-")
        assert delivery.body == delivery.receipt == member
        assert delivery.attempt == 1
    else:
        assert delivery.message_id == "boundary"
        assert delivery.attempt == attempts + 1
    consumer.complete(delivery)


@pytest.mark.parametrize("member", [b"not UTF-8: \xff", b'{"id":"x","attempts":0,"body":"\xff"}'])
def test_non_utf8_reservations_reach_terminal_failure_and_keep_exact_receipts(broker, member):
    from laravel_cloud_queues.errors import MalformedEnvelopeError
    from laravel_cloud_queues.jobs.envelope import decode_envelope

    config, client, _, consumer = broker
    pending = f"{config.prefix}queues:nonutf8"
    client.rpush(pending, member)
    delivery = consumer.receive(["nonutf8"], 0)
    assert delivery is not None
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(delivery.body)
    receipt = delivery.receipt.encode("utf-8", errors="surrogateescape")
    assert client.zscore(pending + ":reserved", receipt) is not None
    consumer.renew(delivery, 60)
    consumer.release(delivery, 0)
    assert client.zscore(pending + ":delayed", receipt) is not None
    redelivered = consumer.receive(["nonutf8"], 0)
    assert redelivered is not None
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(redelivered.body)
    consumer.complete(redelivered)
    assert client.zcard(pending + ":reserved") == client.zcard(pending + ":delayed") == 0


@pytest.mark.parametrize("required", ["1", "true", "TRUE", "yes", "YeS"])
def test_required_service_gate_accepts_truthy_values(monkeypatch, required):
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES", required)
    try:
        with pytest.raises(pytest.fail.Exception):
            unavailable("missing service")
    except pytest.skip.Exception as exc:
        raise AssertionError("Required service must fail instead of skipping") from exc
