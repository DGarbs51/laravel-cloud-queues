"""Verify native asyncio Redis command safety and shutdown behavior."""

from __future__ import annotations

import asyncio
import builtins
import json
import traceback
from unittest.mock import AsyncMock, Mock

import pytest
import redis

from laravel_cloud_queues.config import RedisConfig
from laravel_cloud_queues.errors import (
    AmbiguousAcknowledgementError,
    BrokerConnectionError,
    ConfigurationError,
    LeaseLostError,
    TransportError,
)
from laravel_cloud_queues.transports.base import (
    AsyncConsumer,
    AsyncProducer,
    Delivery,
    OutgoingMessage,
)
from laravel_cloud_queues.transports.redis import AsyncRedisConsumer, AsyncRedisProducer

CONFIG = RedisConfig(url="redis://user:secret@localhost/15")
DELIVERY = Delivery("id", "default", "opaque", 1, receipt="private receipt")
WRAPPER = json.dumps({"id": "id", "body": "opaque", "attempts": 1})
pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def mock_pool(transport):
    connection = Mock(send_command=AsyncMock(), read_response=AsyncMock(), disconnect=AsyncMock())
    pool = Mock(
        get_connection=AsyncMock(return_value=connection),
        release=AsyncMock(),
        disconnect=AsyncMock(),
    )
    transport._pool = transport._reporting_pool = pool
    return pool, connection


@pytest.mark.parametrize("constructor", [AsyncRedisProducer, AsyncRedisConsumer])
async def test_optional_import_is_lazy(monkeypatch, constructor):
    original = builtins.__import__

    def without_redis(name, *args, **kwargs):
        if name == "redis" or name.startswith("redis."):
            raise ImportError("not installed")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_redis)
    with pytest.raises(ConfigurationError, match=r'pip install "laravel-cloud-queues\[redis\]"'):
        constructor(CONFIG)


async def test_capabilities_and_pool_options():
    config = RedisConfig(url="rediss://localhost/15?ssl_ca_certs=%2Ftmp%2Fca.pem")
    producer = AsyncRedisProducer(config)
    consumer = AsyncRedisConsumer(config, lease_seconds=123)
    try:
        assert isinstance(producer, AsyncProducer)
        assert isinstance(consumer, AsyncConsumer)
        assert producer.max_payload_bytes is None
        assert producer.supports_fifo is False
        assert consumer.supports_renewal is True
        assert consumer.blocking is consumer.blocking
        assert consumer.blocking._lease_seconds == 123
        for transport in (producer, consumer, consumer.blocking):
            for pool, timeout in ((transport._pool, 2), (transport._reporting_pool, 10)):
                options = pool.connection_kwargs
                assert options["socket_timeout"] == timeout
                assert options["socket_connect_timeout"] == 2
                assert options["decode_responses"] is True
                assert options["encoding_errors"] == "surrogateescape"
                assert options["ssl_cert_reqs"] == "required"
                assert options["ssl_check_hostname"] is True
                assert options["ssl_ca_certs"] == "/tmp/ca.pem"
                assert options["retry_on_error"] == []
                assert options["retry_on_timeout"] is False
        assert not consumer.blocking._pool._in_use_connections
    finally:
        await producer.aclose()
        await consumer.aclose()


@pytest.mark.parametrize(
    "url",
    [
        "http://user:secret@localhost",
        "redis://user:secret@localhost:invalid",
        "rediss://user:secret@localhost?ssl_cert_reqs=none",
        "rediss://user:secret@localhost?ssl_cert_reqs=optional",
        "rediss://user:secret@localhost?ssl_check_hostname=false",
    ],
)
async def test_invalid_url_is_sanitized(url):
    with pytest.raises(ConfigurationError) as caught:
        AsyncRedisProducer(RedisConfig(url=url))
    assert "secret" not in "".join(traceback.format_exception(caught.value))


async def test_url_cannot_enable_io_or_retry_options():
    producer = AsyncRedisProducer(
        RedisConfig(url="redis://localhost?socket_timeout=999&retry_on_timeout=true")
    )
    try:
        options = producer._pool.connection_kwargs
        assert options["socket_timeout"] == options["socket_connect_timeout"] == 2
        attempt = AsyncMock(side_effect=redis.ConnectionError("unavailable"))
        with pytest.raises(redis.ConnectionError):
            await options["retry"].call_with_retry(attempt, AsyncMock())
        attempt.assert_awaited_once()
    finally:
        await producer.aclose()


@pytest.mark.parametrize("legacy", [False, True])
async def test_acquisition_retries_but_command_is_sent_once(monkeypatch, legacy):
    producer = AsyncRedisProducer(CONFIG)
    pool, connection = mock_pool(producer)
    producer._legacy_pool = legacy
    pool.get_connection.side_effect = [redis.ConnectionError(), redis.TimeoutError(), connection]
    sleep = AsyncMock()
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.anyio.sleep", sleep)
    sent = await producer.send(OutgoingMessage("opaque", "default", delay_seconds=9))
    assert sent.message_id
    assert sent.queue == "default"
    assert [call.args for call in sleep.await_args_list] == [(0.05,), (0.1,)]
    assert pool.get_connection.await_count == 3
    assert all(
        call.args == (("EVAL",) if legacy else ()) for call in pool.get_connection.call_args_list
    )
    connection.send_command.assert_awaited_once()
    command = connection.send_command.call_args.args
    assert command[0] == "EVAL"
    assert command[-1] == 9
    assert json.loads(command[-2]) == {"id": sent.message_id, "attempts": 0, "body": "opaque"}
    pool.release.assert_awaited_once_with(connection)


@pytest.mark.parametrize("operation", ["send", "receive", "complete", "release", "renew"])
@pytest.mark.parametrize("stage", ["connect", "send", "read"])
async def test_connection_failures_are_bounded_classified_and_secret_safe(
    monkeypatch, operation, stage
):
    transport = AsyncRedisProducer(CONFIG) if operation == "send" else AsyncRedisConsumer(CONFIG)
    pool, connection = mock_pool(transport)
    failure = redis.ConnectionError("secret URL and private receipt")
    target = {
        "connect": pool.get_connection,
        "send": connection.send_command,
        "read": connection.read_response,
    }[stage]
    target.side_effect = failure
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.anyio.sleep", AsyncMock())
    reporting = operation in {"complete", "release", "renew"}
    expected = (
        (BrokerConnectionError if stage == "connect" else AmbiguousAcknowledgementError)
        if reporting
        else TransportError
    )
    args = {
        "send": (OutgoingMessage("opaque", "default"),),
        "receive": (["default"], 0),
        "complete": (DELIVERY,),
        "release": (DELIVERY, 3),
        "renew": (DELIVERY, 60),
    }[operation]
    with pytest.raises(expected) as caught:
        await getattr(transport, operation)(*args)
    assert str(failure) not in "".join(traceback.format_exception(caught.value))
    assert pool.get_connection.await_count == (3 if stage == "connect" else 1)
    assert connection.send_command.await_count == (0 if stage == "connect" else 1)
    if stage != "connect":
        connection.disconnect.assert_awaited_once()
        pool.release.assert_awaited_once_with(connection)


@pytest.mark.parametrize("reporting", [False, True])
@pytest.mark.parametrize("stage", ["connect", "send", "read"])
@pytest.mark.parametrize(
    "failure",
    [
        redis.AuthenticationError("private credentials"),
        redis.exceptions.AuthenticationWrongNumberOfArgsError("private credentials"),
        redis.ResponseError("NOAUTH private credentials"),
        redis.ResponseError("WRONGPASS private credentials"),
    ],
)
async def test_authentication_errors_are_configuration_errors(
    monkeypatch, reporting, stage, failure
):
    transport = AsyncRedisConsumer(CONFIG)
    pool, connection = mock_pool(transport)
    target = {
        "connect": pool.get_connection,
        "send": connection.send_command,
        "read": connection.read_response,
    }[stage]
    target.side_effect = failure
    sleep = AsyncMock()
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.anyio.sleep", sleep)
    with pytest.raises(ConfigurationError) as caught:
        await transport._command("PING", reporting=reporting)
    assert "private credentials" not in "".join(traceback.format_exception(caught.value))
    pool.get_connection.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.parametrize("failure", [ValueError("secret"), TypeError("secret")])
async def test_invalid_connection_settings_are_configuration_errors(failure):
    transport = AsyncRedisProducer(CONFIG)
    pool, _ = mock_pool(transport)
    pool.get_connection.side_effect = failure
    with pytest.raises(ConfigurationError, match="Invalid Redis connection settings"):
        await transport.send(OutgoingMessage("opaque", "default"))
    pool.get_connection.assert_awaited_once()


@pytest.mark.parametrize("operation", ["complete", "release", "renew"])
@pytest.mark.parametrize("owned", [0, 1])
async def test_reports_use_separate_pool_and_check_ownership(operation, owned):
    consumer = AsyncRedisConsumer(CONFIG)
    _, connection = mock_pool(consumer)
    consumer._pool = Mock(get_connection=AsyncMock(side_effect=AssertionError))
    connection.read_response.return_value = owned
    args = (DELIVERY,) if operation == "complete" else (DELIVERY, 60)
    if owned:
        await getattr(consumer, operation)(*args)
    else:
        with pytest.raises(LeaseLostError):
            await getattr(consumer, operation)(*args)
    consumer._pool.get_connection.assert_not_awaited()
    consumer._reporting_pool.get_connection.assert_awaited_once()
    command = connection.send_command.call_args.args
    assert command[0] == "EVAL"
    assert command[-2] == DELIVERY.receipt
    assert command[-1] == (0 if operation == "complete" else 60)


async def test_missing_receipt_never_issues_a_command():
    consumer = AsyncRedisConsumer(CONFIG)
    pool, _ = mock_pool(consumer)
    with pytest.raises(LeaseLostError):
        await consumer.complete(Delivery("id", "default", "opaque", 1))
    pool.get_connection.assert_not_awaited()


@pytest.mark.parametrize("wait", [-1, float("inf"), float("nan")])
async def test_invalid_wait_does_not_connect(wait):
    consumer = AsyncRedisConsumer(CONFIG)
    pool, _ = mock_pool(consumer)
    with pytest.raises(ValueError):
        await consumer.receive(["default"], wait)
    pool.get_connection.assert_not_awaited()


async def test_invalid_lease():
    with pytest.raises(ConfigurationError):
        AsyncRedisConsumer(CONFIG, lease_seconds=0)
    consumer = AsyncRedisConsumer(CONFIG)
    with pytest.raises(ValueError):
        await consumer.renew(DELIVERY, 0)
    await consumer.aclose()


async def test_empty_and_interrupted_receive_do_not_connect():
    consumer = AsyncRedisConsumer(CONFIG)
    pool, _ = mock_pool(consumer)
    assert await consumer.receive([], 1) is None
    consumer.interrupt()
    assert await consumer.receive(["default"], 1) is None
    pool.get_connection.assert_not_awaited()
    await consumer.aclose()


@pytest.mark.parametrize(
    "reply",
    [42, b"bytes", [], ["one", "two"], [1], '{"id":1}', '{"id":"x"}', "[]"],
)
async def test_invalid_reservation_replies_are_transport_errors(reply):
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._command = AsyncMock(return_value=reply)
    with pytest.raises(TransportError, match="Invalid Redis reservation response"):
        await consumer.receive(["default"], 0)
    await consumer.aclose()


@pytest.mark.parametrize("reply", [WRAPPER, ["invalid"], "invalid \udcff"])
async def test_reservation_reply_parsing(reply):
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._command = AsyncMock(side_effect=[None, reply])
    delivery = await consumer.receive(["high", "low"], 0)
    assert delivery.queue == "low"
    assert delivery.receipt == (reply[0] if isinstance(reply, list) else reply)
    assert delivery.attempt == 1
    assert delivery.body == ("opaque" if reply == WRAPPER else delivery.receipt)
    await consumer.aclose()


@pytest.mark.parametrize("operation", ["send", "receive", "complete", "release", "renew"])
@pytest.mark.parametrize("suffix", ["delayed", "reserved", "notify"])
async def test_key_aliases_are_rejected_before_commands(operation, suffix):
    transport = AsyncRedisProducer(CONFIG) if operation == "send" else AsyncRedisConsumer(CONFIG)
    transport._command = AsyncMock()
    queue = f"orders:{suffix}"
    delivery = Delivery("id", queue, "opaque", 1, receipt="receipt")
    args = {
        "send": (OutgoingMessage("opaque", queue),),
        "receive": (["orders", queue], 0),
        "complete": (delivery,),
        "release": (delivery, 3),
        "renew": (delivery, 60),
    }[operation]
    with pytest.raises(ConfigurationError):
        await getattr(transport, operation)(*args)
    transport._command.assert_not_awaited()
    await transport.aclose()


async def test_blpop_uses_notify_keys_and_one_second_slices():
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._command = AsyncMock(side_effect=[None, None, None, WRAPPER])
    delivery = await consumer.receive(["high", "low"], 10)
    assert delivery.queue == "high"
    assert consumer._command.call_args_list[2].args == (
        "BLPOP",
        f"{CONFIG.prefix}queues:high:notify",
        f"{CONFIG.prefix}queues:low:notify",
        1.0,
    )
    await consumer.aclose()


@pytest.mark.parametrize("reply", [None, WRAPPER])
async def test_interrupt_does_not_cancel_inflight_reserve(reply):
    consumer = AsyncRedisConsumer(CONFIG)
    started, finish = asyncio.Event(), asyncio.Event()

    async def reserve(*args, **kwargs):
        assert args[0] == "EVAL"
        started.set()
        await finish.wait()
        return reply

    consumer._command = AsyncMock(side_effect=reserve)
    receive = asyncio.create_task(consumer.receive(["default"], 10))
    await started.wait()
    consumer.interrupt()
    await asyncio.sleep(0)
    assert not receive.done()
    finish.set()
    delivery = await receive
    assert (delivery is None) == (reply is None)
    await consumer.aclose()


@pytest.mark.parametrize("interrupt", [False, True])
async def test_cancelled_blpop_discards_connection_before_releasing(interrupt):
    consumer = AsyncRedisConsumer(CONFIG)
    pool, connection = mock_pool(consumer)
    waiting = asyncio.Event()
    calls = []

    async def read():
        if connection.send_command.call_args.args[0] == "EVAL":
            return None
        waiting.set()
        await asyncio.Event().wait()

    async def disconnected():
        calls.append("disconnect")

    async def released(conn):
        calls.append("release")

    connection.read_response.side_effect = read
    connection.disconnect.side_effect = disconnected
    pool.release.side_effect = released
    receive = asyncio.create_task(consumer.receive(["default"], 10))
    await waiting.wait()
    if interrupt:
        consumer.interrupt()
        assert await receive is None
    else:
        receive.cancel()
        with pytest.raises(asyncio.CancelledError):
            await receive
    assert calls == ["release", "disconnect", "release"]
    assert consumer._wait is None
    await consumer.aclose()


async def test_interrupt_between_wait_creation_and_await():
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._command = AsyncMock(return_value=None)
    consumer._interrupted = Mock(is_set=Mock(side_effect=[False, False, True, True]))
    assert await consumer.receive(["default"], 10) is None
    assert consumer._wait is None
    await consumer.aclose()


async def test_interrupt_after_notification_returns_without_reserving_again():
    consumer = AsyncRedisConsumer(CONFIG)

    async def command(*args, **kwargs):
        if args[0] == "BLPOP":
            consumer._interrupted.set()
        return None

    consumer._command = AsyncMock(side_effect=command)
    assert await consumer.receive(["default"], 10) is None
    assert consumer._command.await_count == 2
    await consumer.aclose()


async def test_empty_receive_returns_at_deadline():
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._command = AsyncMock(return_value=None)
    assert await consumer.receive(["default"], 0) is None
    consumer._command.assert_awaited_once()
    await consumer.aclose()


async def test_shutdown_closes_both_pools_and_sync_twin():
    consumer = AsyncRedisConsumer(CONFIG)
    poll, reporting = Mock(disconnect=AsyncMock()), Mock(disconnect=AsyncMock())
    consumer._pool, consumer._reporting_pool = poll, reporting
    consumer._blocking.close = Mock()
    await consumer.aclose()
    poll.disconnect.assert_awaited_once()
    reporting.disconnect.assert_awaited_once()
    consumer.blocking.close.assert_called_once()


async def test_interrupt_tolerates_a_closed_loop():
    consumer = AsyncRedisConsumer(CONFIG)
    consumer._loop = Mock(call_soon_threadsafe=Mock(side_effect=RuntimeError("closed")))
    consumer.interrupt()
    assert consumer._interrupted.is_set()
    await consumer.aclose()


async def test_unicode_command_failure_discards_connection():
    producer = AsyncRedisProducer(CONFIG)
    pool, connection = mock_pool(producer)
    connection.send_command.side_effect = UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogate")
    with pytest.raises(TransportError):
        await producer.send(OutgoingMessage("body", "\ud800"))
    connection.disconnect.assert_awaited_once()
    pool.release.assert_awaited_once_with(connection)
