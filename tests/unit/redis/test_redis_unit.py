from __future__ import annotations

import builtins
import importlib
import traceback
from unittest.mock import Mock

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
from laravel_cloud_queues.transports.base import Delivery, OutgoingMessage
from laravel_cloud_queues.transports.redis import RedisConsumer, RedisProducer

CONFIG = RedisConfig(url="redis://user:secret@127.0.0.1:6379/15")
DELIVERY = Delivery("id", "default", "opaque", 1, receipt="private receipt")


@pytest.mark.parametrize("constructor", [RedisProducer, RedisConsumer])
def test_optional_import_is_lazy(monkeypatch, constructor):
    original = builtins.__import__

    def without_redis(name, *args, **kwargs):
        if name == "redis" or name.startswith("redis."):
            raise ImportError("not installed")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_redis)
    module = importlib.import_module("laravel_cloud_queues.transports.redis")
    importlib.reload(module)
    with pytest.raises(ConfigurationError, match=r'pip install "laravel-cloud-queues\[redis\]"'):
        constructor(CONFIG)


def test_capabilities_and_tls_options():
    producer = RedisProducer(RedisConfig(url="rediss://localhost/15?ssl_ca_certs=%2Ftmp%2Fca.pem"))
    assert producer.max_payload_bytes is None
    assert producer.supports_fifo is False
    options = producer._pool.connection_kwargs
    assert options["ssl_cert_reqs"] == "required"
    assert options["ssl_check_hostname"] is True
    assert options["ssl_ca_certs"] == "/tmp/ca.pem"
    assert RedisConsumer(CONFIG).supports_renewal is True
    assert producer._pool.connection_class.__name__ == "SSLConnection"


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
def test_invalid_or_insecure_url_is_sanitized(url):
    with pytest.raises(ConfigurationError) as caught:
        RedisProducer(RedisConfig(url=url))
    assert "secret" not in "".join(traceback.format_exception(caught.value))


def test_url_cannot_enable_unbounded_io_or_command_retries():
    producer = RedisProducer(
        RedisConfig(url="redis://localhost?socket_timeout=999&retry_on_timeout=true")
    )
    options = producer._pool.connection_kwargs
    assert options["socket_timeout"] == options["socket_connect_timeout"] == 2
    assert options["retry_on_timeout"] is False
    assert options["retry_on_error"] == []
    attempt = Mock(side_effect=redis.ConnectionError("unavailable"))
    with pytest.raises(redis.ConnectionError):
        options["retry"].call_with_retry(attempt, Mock())
    attempt.assert_called_once()


def test_safe_connection_retry_and_backoff(monkeypatch):
    producer = RedisProducer(CONFIG)
    connection = Mock()
    producer._pool = Mock()
    producer._pool.get_connection.side_effect = [
        redis.ConnectionError("secret"),
        redis.TimeoutError("secret"),
        connection,
    ]
    sleep = Mock()
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.time.sleep", sleep)
    sent = producer.send(OutgoingMessage("opaque", "default"))
    assert sent.message_id
    assert producer._pool.get_connection.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [0.05, 0.1]
    connection.send_command.assert_called_once()
    producer._pool.release.assert_called_once_with(connection)


@pytest.mark.parametrize("operation", ["receive", "send", "complete", "release", "renew"])
@pytest.mark.parametrize("stage", ["connect", "send", "read"])
def test_connection_failures_are_bounded_classified_and_secret_safe(monkeypatch, operation, stage):
    transport = RedisProducer(CONFIG) if operation == "send" else RedisConsumer(CONFIG)
    connection = Mock()
    transport._pool = transport._reporting_pool = Mock()
    transport._pool.get_connection.return_value = connection
    failure = redis.ConnectionError("secret URL and private receipt")
    if stage == "connect":
        transport._pool.get_connection.side_effect = failure
    elif stage == "send":
        connection.send_command.side_effect = failure
    else:
        connection.read_response.side_effect = failure
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.time.sleep", Mock())
    reporting = operation in {"complete", "release", "renew"}
    expected = (
        (BrokerConnectionError if stage == "connect" else AmbiguousAcknowledgementError)
        if reporting
        else TransportError
    )
    args = {
        "receive": (["default"], 0),
        "send": (OutgoingMessage("opaque", "default"),),
        "complete": (DELIVERY,),
        "release": (DELIVERY, 3),
        "renew": (DELIVERY, 60),
    }[operation]
    with pytest.raises(expected) as caught:
        getattr(transport, operation)(*args)
    assert str(failure) not in "".join(traceback.format_exception(caught.value))
    assert "private receipt" not in str(caught.value)
    assert transport._pool.get_connection.call_count == (3 if stage == "connect" else 1)
    assert connection.send_command.call_count == (0 if stage == "connect" else 1)
    if stage != "connect":
        connection.disconnect.assert_called_once()
        transport._pool.release.assert_called_once_with(connection)


def test_missing_receipt_never_issues_a_command():
    consumer = RedisConsumer(CONFIG)
    consumer._pool = Mock()
    with pytest.raises(LeaseLostError):
        consumer.complete(Delivery("id", "default", "opaque", 1))
    consumer._pool.get_connection.assert_not_called()


@pytest.mark.parametrize("wait", [-1, float("inf"), float("nan")])
def test_invalid_wait_does_not_block(wait):
    with pytest.raises(ValueError):
        RedisConsumer(CONFIG).receive(["default"], wait)


def test_empty_queues_and_interrupted_receive_do_not_connect():
    consumer = RedisConsumer(CONFIG)
    consumer._pool = Mock()
    assert consumer.receive([], 1) is None
    consumer.interrupt()
    assert consumer.receive(["default"], 1) is None
    consumer._pool.get_connection.assert_not_called()


def test_invalid_lease():
    with pytest.raises(ConfigurationError):
        RedisConsumer(CONFIG, lease_seconds=0)
    with pytest.raises(ValueError):
        RedisConsumer(CONFIG).renew(DELIVERY, 0)


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
def test_authentication_errors_are_configuration_errors(monkeypatch, reporting, stage, failure):
    transport = RedisConsumer(CONFIG)
    connection = Mock()
    pool = Mock()
    transport._pool = transport._reporting_pool = pool
    pool.get_connection.return_value = connection
    target = {
        "connect": pool.get_connection,
        "send": connection.send_command,
        "read": connection.read_response,
    }[stage]
    target.side_effect = failure
    sleep = Mock()
    monkeypatch.setattr("laravel_cloud_queues.transports.redis.time.sleep", sleep)
    with pytest.raises(ConfigurationError) as caught:
        transport._command("PING", reporting=reporting)
    assert "private credentials" not in "".join(traceback.format_exception(caught.value))
    pool.get_connection.assert_called_once()
    sleep.assert_not_called()


@pytest.mark.parametrize("operation", ["complete", "release", "renew"])
def test_reporting_timeout_is_separate_from_polling(monkeypatch, operation):
    consumer = RedisConsumer(CONFIG)
    try:
        assert consumer._pool.connection_kwargs["socket_timeout"] == 2
        assert consumer._reporting_pool.connection_kwargs["socket_timeout"] == 10
        for pool in (consumer._pool, consumer._reporting_pool):
            connection = pool.make_connection()
            assert connection.socket_timeout == pool.connection_kwargs["socket_timeout"]
        connection = Mock()
        connection.read_response.return_value = 1
        acquire = Mock(return_value=connection)
        release = Mock()
        monkeypatch.setattr(consumer._reporting_pool, "get_connection", acquire)
        monkeypatch.setattr(consumer._reporting_pool, "release", release)
        monkeypatch.setattr(consumer._pool, "get_connection", Mock(side_effect=AssertionError))
        args = (DELIVERY,) if operation == "complete" else (DELIVERY, 60)
        getattr(consumer, operation)(*args)
        acquire.assert_called_once()
        release.assert_called_once_with(connection)
    finally:
        consumer.close()


def test_legacy_pools_receive_the_command_name():
    """redis-py < 5.3 requires the command name when acquiring a connection."""
    consumer = RedisConsumer(CONFIG)
    assert consumer._legacy_pool is False
    connection = Mock()
    connection.read_response.return_value = "PONG"
    consumer._pool = Mock()
    consumer._pool.get_connection.return_value = connection
    assert consumer._command("PING") == "PONG"
    consumer._pool.get_connection.assert_called_once_with()
    consumer._legacy_pool = True
    consumer._pool.get_connection.reset_mock()
    assert consumer._command("PING") == "PONG"
    consumer._pool.get_connection.assert_called_once_with("PING")


@pytest.mark.parametrize(
    "reply",
    [42, b"bytes", [], ["one", "two"], [1], '{"id":1,"body":"x","attempts":1}', '{"id":"x"}', "[]"],
)
def test_invalid_reservation_replies_are_transport_errors(monkeypatch, reply):
    consumer = RedisConsumer(CONFIG)
    monkeypatch.setattr(consumer, "_command", Mock(return_value=reply))
    with pytest.raises(TransportError, match="Invalid Redis reservation response"):
        consumer.receive(["default"], 0)
