"""Agent contract: framework/src/Illuminate/Foundation/Cloud/Queue.php:260-373."""

import gc
import json
import traceback
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest

from laravel_cloud_queues.config import (
    AgentConfig,
    ManagedQueuesConfig,
    QueueConfig,
    SqsConnectionConfig,
)
from laravel_cloud_queues.errors import (
    AgentProtocolError,
    AgentUnavailableError,
    ConfigurationError,
)
from laravel_cloud_queues.transports import agent, create_backend
from laravel_cloud_queues.transports.base import SQS_MAX_PAYLOAD_BYTES, Consumer, Delivery

FIXTURES = Path(__file__).parents[2] / "fixtures" / "agent"
RESPONSES = json.loads((FIXTURES / "responses.json").read_text())
REQUESTS = json.loads((FIXTURES / "requests.json").read_text())
FAULTS = json.loads((FIXTURES / "faults.json").read_text())


@pytest.fixture
def mock_agent(monkeypatch):
    requests, responses, pauses = [], [], []

    def handle(request):
        requests.append(request)
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kw: httpx.MockTransport(handle))
    managed = ManagedQueuesConfig(
        connection=SqsConnectionConfig(
            prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
            region="us-east-1",
            credentials="ecs",
            suffix="-prod",
        ),
        agent=AgentConfig(enabled=True, socket="/unused.sock"),
        queue="assigned",
    )
    consumer = agent.AgentConsumer(managed)
    monkeypatch.setattr(agent, "sleep", pauses.append)
    monkeypatch.setattr(consumer._stopping, "wait", lambda delay: pauses.append(delay) or False)
    try:
        yield consumer, requests, responses, pauses
    finally:
        consumer.close()


def response(case):
    return httpx.Response(
        case["status"], content=case["raw"] if "raw" in case else json.dumps(case["body"])
    )


@pytest.mark.parametrize("case", RESPONSES, ids=lambda c: c["name"])
def test_receive_shapes(mock_agent, monkeypatch, case):
    """Queue.php:264-281; Symfony CloudQueueTransport.php:313,359; D13.5."""
    consumer, requests, responses, pauses = mock_agent
    normalizer = Mock(return_value="emails")  # L3a owns the still-stubbed helper.
    monkeypatch.setattr(agent, "normalize_queue", normalizer)
    responses.append(response(case))
    delivery = consumer.receive(["ignored"], 0)
    if case["expected"] is None:
        assert delivery is None
    else:
        assert delivery is not None
        for key, value in case["expected"].items():
            assert getattr(delivery, key) == value
        assert delivery.received_at > 0
        url = case["body"].get("queueUrl")
        if isinstance(url, str) and url:
            normalizer.assert_called_once_with(consumer._managed.connection, url)
            assert delivery.meta == {"queue_url": url}
        else:
            normalizer.assert_not_called()
            assert delivery.meta == {}
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert str(requests[0].url) == "http://localhost/next"
    assert set(requests[0].extensions["timeout"].values()) == {65}
    assert requests[0].headers["accept-encoding"] == "identity"
    assert pauses == []


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (None, 1),
        ("bad", 1),
        ([], 1),
        ({}, 1),
        ("4", 4),
        (2, 2),
        (0, 1),
        (-2, 1),
        ("0", 1),
        ("1.5", 1),
        (float("inf"), 1),
        (2.0, 2),
    ],
)
def test_receive_count(mock_agent, count, expected):
    """Symfony CloudQueueTransport.php:359; missing-receive-count-is-one deviation."""
    consumer, _, responses, _ = mock_agent
    responses.append(
        httpx.Response(
            200,
            json={
                "messageId": "m",
                "attributes": {"ApproximateReceiveCount": count},
            },
        )
        if count != float("inf")
        else httpx.Response(
            200, content=('{"messageId":"m","attributes":{"ApproximateReceiveCount":1e999}}')
        )
    )
    assert consumer.receive([], 0).attempt == expected


@pytest.mark.parametrize("attributes", [None, [], "bad", 42, {}])
def test_malformed_attributes(mock_agent, attributes):
    consumer, _, responses, _ = mock_agent
    responses.append(httpx.Response(200, json={"messageId": "m", "attributes": attributes}))
    assert consumer.receive([], 0).attempt == 1


@pytest.mark.parametrize("case", REQUESTS, ids=lambda c: c["name"])
def test_result_payload(mock_agent, case):
    """Queue.php:339-344: only nulls omitted, zero delay and empty receipt preserved."""
    consumer, requests, responses, pauses = mock_agent
    delivery = Delivery(queue="assigned", body="secret", attempt=1, **case["delivery"])
    responses.append(httpx.Response(200))
    if case["delay"] is None:
        consumer.complete(delivery)
    else:
        consumer.release(delivery, case["delay"])
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert str(requests[0].url) == "http://localhost/result"
    assert json.loads(requests[0].content) == case["body"]
    assert set(requests[0].extensions["timeout"].values()) == {10}
    assert pauses == []
    with pytest.raises(RuntimeError, match="already"):
        consumer.complete(delivery)
    with pytest.raises(RuntimeError, match="already"):
        consumer.release(delivery, 0)
    assert len(requests) == 1


@pytest.mark.parametrize("case", FAULTS, ids=lambda c: c["name"])
def test_fault_status_shape_and_attempts(mock_agent, case):
    """Queue.php:294-356, D13.3: HTTP error retries only for polls."""
    consumer, requests, responses, pauses = mock_agent
    responses.extend(response(case) for _ in range(case["attempts"]))
    delivery = Delivery("m", "assigned", "payload-secret", 1, receipt="receipt-secret")
    error = AgentProtocolError if case["error"] == "AgentProtocolError" else AgentUnavailableError
    with pytest.raises(error) as exc:
        consumer.receive([], 0) if case["endpoint"] == "next" else consumer.complete(delivery)
    assert len(requests) == case["attempts"]
    assert pauses == ([0.5] if case["attempts"] == 3 else [])
    assert "secret" not in "".join(traceback.format_exception(exc.type, exc.value, exc.tb))
    if isinstance(exc.value, AgentProtocolError):
        assert exc.value.status == case["status"]
    else:
        assert exc.value.exit_code == 0
    if case["endpoint"] == "result":
        with pytest.raises(RuntimeError, match="already"):
            consumer.release(delivery, 0)
        assert len(requests) == 1


@pytest.mark.parametrize("code", [400, 404, 429, 500, 503, 599])
@pytest.mark.parametrize("failures", [1, 2])
def test_receive_recovers_from_http_errors(mock_agent, code, failures):
    """Queue.php:299, D13.3: immediate retry then 500 ms, stopping on success."""
    consumer, requests, responses, pauses = mock_agent
    responses.extend(httpx.Response(code) for _ in range(failures))
    responses.append(httpx.Response(204))
    assert consumer.receive([], 999) is None
    assert len(requests) == failures + 1
    assert pauses == ([0.5] if failures == 2 else [])


@pytest.mark.parametrize("endpoint", ["next", "result"])
@pytest.mark.parametrize("failures", [1, 2, 3])
@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ReadError,
        httpx.ReadTimeout,
        httpx.WriteError,
        httpx.WriteTimeout,
        httpx.RemoteProtocolError,
        httpx.PoolTimeout,
    ],
)
def test_connection_retries(mock_agent, endpoint, failures, error):
    """Queue.php:299,338; QueueTest.php:1206 (poll read timeouts are retried)."""
    consumer, requests, responses, pauses = mock_agent
    responses.extend(error("receipt-secret payload-secret") for _ in range(failures))
    responses.append(httpx.Response(204 if endpoint == "next" else 200))
    delivery = Delivery("m", "q", "secret", 1)

    def call():
        return consumer.receive([], 0) if endpoint == "next" else consumer.complete(delivery)

    if failures == 3:
        with pytest.raises(AgentUnavailableError) as exc:
            call()
        assert "secret" not in "".join(traceback.format_exception(exc.type, exc.value, exc.tb))
    else:
        assert call() is None
    attempts = min(failures + 1, 3)
    assert len(requests) == attempts
    assert pauses == (
        [0.1] * (attempts - 1) if endpoint == "result" else [0.5] if attempts == 3 else []
    )
    if endpoint == "result":
        assert len({r.content for r in requests}) == 1
        with pytest.raises(RuntimeError):
            consumer.release(delivery, 0)


@pytest.mark.parametrize("endpoint", ["next", "result"])
def test_other_http_errors_are_sanitized_without_retries(mock_agent, endpoint):
    consumer, requests, responses, pauses = mock_agent
    responses.append(httpx.LocalProtocolError("payload-secret"))
    with pytest.raises(AgentUnavailableError) as exc:
        consumer.receive([], 0) if endpoint == "next" else consumer.complete(
            Delivery("m", "q", "", 1)
        )
    assert "secret" not in "".join(traceback.format_exception(exc.type, exc.value, exc.tb))
    assert len(requests) == 1
    assert pauses == []


@pytest.mark.parametrize("code", [200, 201, 204, 302, 399])
def test_result_non_errors_are_accepted_without_following_redirects(mock_agent, code):
    """Symfony AgentClient.php:126-135: only >=400 is failure; no redirect following."""
    consumer, requests, responses, _ = mock_agent
    responses.append(httpx.Response(code, headers={"Location": "http://elsewhere/"}))
    consumer.complete(Delivery("m", "q", "", 1))
    assert len(requests) == 1


@pytest.mark.parametrize("raw", [b"\xff", b"[" * 2000, b""])
def test_invalid_json_is_sanitized(mock_agent, raw):
    consumer, requests, responses, _ = mock_agent
    responses.append(httpx.Response(200, content=raw))
    with pytest.raises(AgentUnavailableError, match="invalid JSON"):
        consumer.receive([], 0)
    assert len(requests) == 1


@pytest.mark.parametrize("oversize", [False, True])
def test_streamed_response_limit(mock_agent, oversize):
    consumer, requests, responses, _ = mock_agent
    consumed, closed = [], []

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield b'{"messageId":"m","body":"'
            remaining = agent._MAX_RESPONSE_BYTES - len(b'{"messageId":"m","body":""}')
            while remaining:
                size = min(remaining, 65536)
                consumed.append(size)
                yield b"x" * size
                remaining -= size
            yield b'x"}' if oversize else b'"}'
            if oversize:
                pytest.fail("Stream read beyond response cap")

        def close(self):
            closed.append(True)

    responses.append(httpx.Response(200, stream=Stream()))
    if oversize:
        with pytest.raises(AgentUnavailableError, match="size limit"):
            consumer.receive([], 0)
    else:
        assert consumer.receive([], 0).message_id == "m"
    assert len(requests) == 1
    assert len(consumed) > 1
    assert closed == [True]


def test_response_cap_admits_the_largest_legal_sqs_body(mock_agent):
    """R4 m5: a 1 MiB body of escaped quotes doubles under the agent's JSON escaping.
    The cap must admit it, because rejecting it is fatal and the message is redelivered
    to every restarted worker."""
    consumer, requests, responses, _ = mock_agent
    body = json.dumps({"displayName": "j", "laravel_cloud_queues": {"args": ['"' * 600_000]}})
    body = body[: SQS_MAX_PAYLOAD_BYTES - 2] + '"}'
    assert len(body.encode()) <= SQS_MAX_PAYLOAD_BYTES
    raw = json.dumps({"messageId": "big", "receiptHandle": "r" * 1024, "body": body}).encode()
    assert len(raw) > 2 * 1024 * 1024
    responses.append(httpx.Response(200, content=raw))
    delivery = consumer.receive([], 0)
    assert delivery.message_id == "big"
    assert delivery.body == body
    assert len(requests) == 1


@pytest.mark.parametrize(("method", "status"), [("next", 204), ("result", 200), ("result", 422)])
def test_unused_response_bodies_are_not_read(mock_agent, method, status):
    consumer, _, responses, _ = mock_agent

    class Unreadable(httpx.SyncByteStream):
        def __iter__(self):
            pytest.fail("Unused body must not be read")
            yield b""

    responses.append(httpx.Response(status, stream=Unreadable()))
    if method == "next":
        assert consumer.receive([], 0) is None
    elif status == 422:
        with pytest.raises(AgentProtocolError):
            consumer.complete(Delivery("m", "q", "", 1))
    else:
        consumer.complete(Delivery("m", "q", "", 1))


def test_rejects_unrequested_compression(mock_agent):
    consumer, _, responses, _ = mock_agent
    responses.append(
        httpx.Response(200, headers={"content-encoding": "gzip"}, stream=httpx.ByteStream(b""))
    )
    with pytest.raises(AgentUnavailableError, match="encoding"):
        consumer.receive([], 0)


def test_report_tracking_does_not_retain_payloads(mock_agent):
    consumer, _, responses, _ = mock_agent
    delivery = Delivery("m", "q", "", 1)
    responses.append(httpx.Response(200))
    consumer.complete(delivery)
    assert len(consumer._reported) == 1
    del delivery
    gc.collect()
    assert len(consumer._reported) == 0


def test_interrupt_preserves_full_response_and_reporting(mock_agent, monkeypatch):
    consumer, requests, responses, _ = mock_agent
    decode = consumer._delivery

    def interrupt_then_decode(body):
        consumer.interrupt()
        return decode(body)

    monkeypatch.setattr(consumer, "_delivery", interrupt_then_decode)
    responses.extend([httpx.Response(200, json={"messageId": "m"}), httpx.Response(200)])
    delivery = consumer.receive([], 0)
    assert delivery.message_id == "m"
    consumer.interrupt()
    consumer.complete(delivery)
    assert consumer.receive([], 0) is None
    assert len(requests) == 2


def test_interrupt_never_aborts_an_in_flight_poll(mock_agent):
    """Laravel parity: a message the agent hands over after the stop
    signal is returned and run, never dropped by closing the socket under the agent."""
    consumer, requests, responses, _ = mock_agent

    class HandoverAfterInterrupt(httpx.SyncByteStream):
        def __iter__(self):
            consumer.interrupt()
            yield b'{"messageId":"late","receiptHandle":"r","body":"{}"}'

        def close(self):
            pass

    responses.append(httpx.Response(200, stream=HandoverAfterInterrupt()))
    delivery = consumer.receive([], 0)
    assert delivery is not None and delivery.message_id == "late"
    assert consumer.receive([], 0) is None
    assert len(requests) == 1


def test_renewal_and_lifecycle(mock_agent):
    consumer, requests, _, _ = mock_agent
    assert isinstance(consumer, Consumer)
    assert consumer.supports_renewal is False
    assert consumer.renew(Delivery("m", "q", "", 1), 60) is None
    consumer.interrupt()
    consumer.interrupt()
    assert consumer.receive([], 0) is None
    assert requests == []
    consumer.close()
    consumer.close()
    assert consumer._client.is_closed


def test_partial_response_disconnect_retries_with_fresh_body(mock_agent):
    consumer, requests, responses, pauses = mock_agent

    class Disconnected(httpx.SyncByteStream):
        def __iter__(self):
            yield b'{"messageId":"partial-secret'
            raise httpx.ReadError("receipt-secret")

    responses.extend(
        [
            httpx.Response(200, stream=Disconnected()),
            httpx.Response(200, json={"messageId": "complete"}),
        ]
    )
    assert consumer.receive([], 0).message_id == "complete"
    assert len(requests) == 2
    assert pauses == []


def test_interrupt_during_retry_backoff(mock_agent, monkeypatch):
    consumer, requests, responses, _ = mock_agent
    responses.extend([httpx.Response(503), httpx.Response(503)])

    def wait(delay):
        assert delay == 0.5
        consumer.interrupt()
        return True

    monkeypatch.setattr(consumer._stopping, "wait", wait)
    assert consumer.receive([], 0) is None
    assert len(requests) == 2


def test_interrupt_during_connection_failure_stops_polling(mock_agent, monkeypatch):
    consumer, requests, _, pauses = mock_agent

    def interrupt_then_fail(request):
        requests.append(request)
        consumer.interrupt()
        raise httpx.ConnectError("receipt-secret")

    monkeypatch.setattr(consumer._client, "_transport", httpx.MockTransport(interrupt_then_fail))
    assert consumer.receive([], 0) is None
    assert len(requests) == 1
    assert pauses == []


def test_managed_backend_requires_managed_config():
    connection = SqsConnectionConfig("prefix", "us-east-1", "ecs")
    with pytest.raises(ConfigurationError, match="managed configuration"):
        create_backend(QueueConfig("managed", sqs=connection))
