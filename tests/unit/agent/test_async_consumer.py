"""Async agent consumer: same contract as AgentConsumer, native httpx."""

import asyncio
import json
import threading
import time

import httpx
import pytest

from laravel_cloud_queues.config import AgentConfig, ManagedQueuesConfig, SqsConnectionConfig
from laravel_cloud_queues.errors import AgentProtocolError, AgentUnavailableError
from laravel_cloud_queues.transports import agent
from laravel_cloud_queues.transports.base import SQS_MAX_PAYLOAD_BYTES, AsyncConsumer, Delivery

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def managed_config():
    return ManagedQueuesConfig(
        connection=SqsConnectionConfig(
            prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
            region="us-east-1",
            credentials="ecs",
            suffix="-prod",
        ),
        agent=AgentConfig(enabled=True, socket="/unused.sock"),
        queue="assigned",
    )


@pytest.fixture
async def mock_agent(monkeypatch, anyio_backend):
    del anyio_backend
    requests, responses, pauses, transports = [], [], [], []

    def handle(request):
        requests.append(request)
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def transport(**kwargs):
        transports.append(kwargs)
        return httpx.MockTransport(handle)

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)
    monkeypatch.setattr(httpx, "HTTPTransport", transport)
    consumer = agent.AsyncAgentConsumer(managed_config())
    try:
        yield consumer, requests, responses, pauses, transports
    finally:
        await consumer.aclose()


async def test_client_options_and_lifecycle(mock_agent):
    consumer, requests, _, _, transports = mock_agent
    assert isinstance(consumer, AsyncConsumer)
    assert consumer.supports_renewal is False
    assert await consumer.renew(Delivery("m", "q", "", 1), 60) is None
    assert isinstance(consumer.blocking, agent.AgentConsumer)
    assert consumer.blocking._claims is consumer._claims
    assert transports[0]["uds"] == "/unused.sock"
    assert transports[1]["uds"] == "/unused.sock"
    consumer.interrupt()
    consumer.interrupt()
    assert await consumer.receive([], 0) is None
    assert requests == []
    assert consumer.blocking._stopping.is_set()
    await consumer.aclose()
    await consumer.aclose()
    assert consumer._client.is_closed
    assert consumer.blocking._client.is_closed


async def test_receive_message_and_empty_poll(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.append(httpx.Response(200, json={"messageId": "m", "body": "payload"}))
    delivery = await consumer.receive(["ignored"], 999)
    assert delivery is not None
    assert delivery.message_id == "m"
    assert delivery.body == "payload"
    assert delivery.queue == "assigned"
    assert delivery.received_at > 0
    responses.append(httpx.Response(204))
    assert await consumer.receive([], 0) is None
    assert [str(request.url) for request in requests] == [
        "http://localhost/next",
        "http://localhost/next",
    ]
    assert requests[0].headers["accept-encoding"] == "identity"
    assert set(requests[0].extensions["timeout"].values()) == {65}
    assert pauses == []


async def test_missing_message_and_non_retryable_status(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.append(httpx.Response(200, json={"messageId": ""}))
    assert await consumer.receive([], 0) is None
    responses.append(httpx.Response(302))
    with pytest.raises(AgentUnavailableError, match="HTTP 302"):
        await consumer.receive([], 0)
    assert len(requests) == 2
    assert pauses == []


@pytest.mark.parametrize("raw", [b"\xff", b"[" * 2000, b"[]", b"null"])
async def test_invalid_and_non_object_json(mock_agent, raw):
    consumer, _, responses, _pauses, _ = mock_agent
    responses.append(httpx.Response(200, content=raw))
    match = "invalid JSON" if raw != b"[]" and raw != b"null" else "non-object"
    if raw == b"[]":
        assert await consumer.receive([], 0) is None
    else:
        with pytest.raises(AgentUnavailableError, match=match):
            await consumer.receive([], 0)


@pytest.mark.parametrize("failures", [1, 2])
async def test_poll_recovers_from_http_errors(mock_agent, failures):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.extend(httpx.Response(503) for _ in range(failures))
    responses.append(httpx.Response(204))
    assert await consumer.receive([], 0) is None
    assert len(requests) == failures + 1
    assert pauses == []


async def test_poll_http_errors_exhaust(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.extend(httpx.Response(500) for _ in range(3))
    with pytest.raises(AgentUnavailableError, match="HTTP 500"):
        await consumer.receive([], 0)
    assert len(requests) == 3
    assert pauses == []


@pytest.mark.parametrize("failures", [1, 3])
async def test_poll_connection_retries(mock_agent, failures):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.extend(httpx.ConnectError("receipt-secret") for _ in range(failures))
    responses.append(httpx.Response(204))
    if failures == 3:
        with pytest.raises(AgentUnavailableError, match="unreachable") as exc:
            await consumer.receive([], 0)
        assert "secret" not in str(exc.value)
    else:
        assert await consumer.receive([], 0) is None
    assert len(requests) == min(failures + 1, 3)
    assert pauses == []


async def test_other_http_error_is_not_retried(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.append(httpx.LocalProtocolError("payload-secret"))
    with pytest.raises(AgentUnavailableError, match="HTTP request failed") as exc:
        await consumer.receive([], 0)
    assert "secret" not in str(exc.value)
    assert len(requests) == 1
    assert pauses == []


async def test_result_payload_and_shared_claim(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    delivery = Delivery("m", "assigned", "secret", 1, receipt="r")
    responses.append(httpx.Response(204))
    await consumer.complete(delivery)
    assert json.loads(requests[0].content) == {
        "messageId": "m",
        "receiptHandle": "r",
        "status": "processed",
    }
    assert set(requests[0].extensions["timeout"].values()) == {10}
    assert pauses == []
    with pytest.raises(RuntimeError, match="already"):
        await consumer.release(delivery, 0)
    with pytest.raises(RuntimeError, match="already"):
        consumer.blocking.complete(delivery)
    assert len(requests) == 1

    other = Delivery("n", "assigned", "", 1)
    responses.append(httpx.Response(201, headers={"Location": "http://elsewhere/"}))
    consumer.blocking.release(other, 0)
    with pytest.raises(RuntimeError, match="already"):
        await consumer.complete(other)
    assert json.loads(requests[1].content)["delay"] == 0
    assert len(requests) == 2


@pytest.mark.parametrize(
    ("code", "error"), [(422, AgentProtocolError), (503, AgentUnavailableError)]
)
async def test_result_http_errors_are_not_retried(mock_agent, code, error):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.append(httpx.Response(code))
    delivery = Delivery("m", "q", "secret", 1, receipt="receipt-secret")
    with pytest.raises(error) as exc:
        await consumer.complete(delivery)
    assert "secret" not in str(exc.value)
    if isinstance(exc.value, AgentProtocolError):
        assert exc.value.status == code
    assert len(requests) == 1
    assert pauses == []
    with pytest.raises(RuntimeError, match="already"):
        consumer.blocking.release(delivery, 1)


@pytest.mark.parametrize("failures", [1, 3])
async def test_result_connection_retries(mock_agent, failures):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.extend(httpx.ReadError("receipt-secret") for _ in range(failures))
    responses.append(httpx.Response(200))
    delivery = Delivery("m", "q", "", 1)
    if failures == 3:
        with pytest.raises(AgentUnavailableError, match="unreachable"):
            await consumer.complete(delivery)
    else:
        await consumer.complete(delivery)
    assert len(requests) == min(failures + 1, 3)
    assert len({request.content for request in requests}) == 1
    assert pauses == []
    with pytest.raises(RuntimeError, match="already"):
        await consumer.release(delivery, 0)


async def test_result_other_http_error_is_not_retried(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent
    responses.append(httpx.LocalProtocolError("payload-secret"))
    with pytest.raises(AgentUnavailableError, match="HTTP request failed"):
        await consumer.release(Delivery("m", "q", "", 1), 5)
    assert len(requests) == 1
    assert pauses == []


@pytest.mark.parametrize("oversize", [False, True])
async def test_streamed_response_limit(mock_agent, oversize):
    consumer, requests, responses, _, _ = mock_agent
    consumed, closed = [], []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
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

        async def aclose(self):
            closed.append(True)

    responses.append(httpx.Response(200, stream=Stream()))
    if oversize:
        with pytest.raises(AgentUnavailableError, match="size limit"):
            await consumer.receive([], 0)
    else:
        delivery = await consumer.receive([], 0)
        assert delivery is not None and delivery.message_id == "m"
    assert len(requests) == 1
    assert len(consumed) > 1
    assert closed == [True]


async def test_response_cap_admits_the_largest_legal_sqs_body(mock_agent):
    consumer, _, responses, _, _ = mock_agent
    body = json.dumps({"displayName": "j", "laravel_cloud_queues": {"args": ['"' * 600_000]}})
    body = body[: SQS_MAX_PAYLOAD_BYTES - 2] + '"}'
    assert len(body.encode()) <= SQS_MAX_PAYLOAD_BYTES
    raw = json.dumps({"messageId": "big", "receiptHandle": "r" * 1024, "body": body}).encode()
    assert len(raw) > 2 * 1024 * 1024
    responses.append(httpx.Response(200, content=raw))
    delivery = await consumer.receive([], 0)
    assert delivery is not None
    assert delivery.message_id == "big"
    assert delivery.body == body


@pytest.mark.parametrize(("method", "status"), [("next", 204), ("result", 200), ("result", 422)])
async def test_unused_response_bodies_are_not_read(mock_agent, method, status):
    consumer, _, responses, _, _ = mock_agent

    class Unreadable(httpx.AsyncByteStream):
        async def __aiter__(self):
            pytest.fail("Unused body must not be read")
            yield b""

    responses.append(httpx.Response(status, stream=Unreadable()))
    if method == "next":
        assert await consumer.receive([], 0) is None
    elif status == 422:
        with pytest.raises(AgentProtocolError):
            await consumer.complete(Delivery("m", "q", "", 1))
    else:
        await consumer.complete(Delivery("m", "q", "", 1))


async def test_rejects_unrequested_compression(mock_agent):
    consumer, _, responses, _, _ = mock_agent
    responses.append(
        httpx.Response(200, headers={"content-encoding": "gzip"}, stream=httpx.ByteStream(b""))
    )
    with pytest.raises(AgentUnavailableError, match="encoding"):
        await consumer.receive([], 0)


async def test_partial_response_disconnect_retries(mock_agent):
    consumer, requests, responses, pauses, _ = mock_agent

    class Disconnected(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"messageId":"partial-secret'
            raise httpx.ReadError("receipt-secret")

    responses.extend(
        [
            httpx.Response(200, stream=Disconnected()),
            httpx.Response(200, json={"messageId": "complete"}),
        ]
    )
    delivery = await consumer.receive([], 0)
    assert delivery is not None and delivery.message_id == "complete"
    assert len(requests) == 2
    assert pauses == []


async def test_interrupt_preserves_handover_and_reporting(mock_agent):
    consumer, requests, responses, _, _ = mock_agent

    class Handover(httpx.AsyncByteStream):
        async def __aiter__(self):
            consumer.interrupt()
            yield b'{"messageId":"late","receiptHandle":"r","body":"{}"}'

    responses.extend([httpx.Response(200, stream=Handover()), httpx.Response(200)])
    delivery = await consumer.receive([], 0)
    assert delivery is not None and delivery.message_id == "late"
    await consumer.complete(delivery)
    assert await consumer.receive([], 0) is None
    assert len(requests) == 2


async def test_backoff_stays_on_the_loop_and_wakes(mock_agent, monkeypatch):
    consumer, requests, responses, _, _ = mock_agent
    responses.extend([httpx.Response(503), httpx.Response(503)])

    def explode(*_args, **_kwargs):
        raise AssertionError("receive backoff must not call anyio.to_thread.run_sync")

    monkeypatch.setattr(agent.anyio.to_thread, "run_sync", explode)
    started = {}

    def wake():
        deadline = time.monotonic() + 1
        while consumer._backoff is None:
            assert time.monotonic() < deadline
            time.sleep(0.001)
        time.sleep(0.02)
        started["t"] = time.monotonic()
        consumer.interrupt()

    threading.Thread(target=wake, daemon=True).start()
    assert await consumer.receive([], 0) is None
    assert time.monotonic() - started["t"] < 0.1
    assert len(requests) == 2
    assert consumer._backoff is None


async def test_interrupt_during_poll_skips_the_backoff_wait(mock_agent):
    consumer, requests, _, _, _ = mock_agent

    def fail_then_stop(request):
        requests.append(request)
        if len(requests) == 2:
            consumer.interrupt()
        return httpx.Response(503)

    consumer._client._transport = httpx.MockTransport(fail_then_stop)
    started = time.monotonic()
    assert await consumer.receive([], 0) is None
    assert time.monotonic() - started < 0.1
    assert len(requests) == 2


async def test_interrupt_ignores_a_closed_loop(mock_agent):
    consumer, *_ = mock_agent

    class ClosedLoop:
        def call_soon_threadsafe(self, _callback):
            raise RuntimeError("Event loop is closed")

    consumer._loop = ClosedLoop()
    consumer._backoff = asyncio.Event()
    consumer.interrupt()
    assert consumer._stopping.is_set()
    assert consumer.blocking._stopping.is_set()


async def test_interrupt_during_connection_failure_stops_polling(mock_agent):
    consumer, requests, _, pauses, _ = mock_agent

    def interrupt_then_fail(request):
        requests.append(request)
        consumer.interrupt()
        raise httpx.ConnectError("receipt-secret")

    consumer._client._transport = httpx.MockTransport(interrupt_then_fail)
    assert await consumer.receive([], 0) is None
    assert len(requests) == 1
    assert pauses == []


async def test_result_retry_pause_uses_sleep(monkeypatch):
    requests, responses = [], []

    def handle(request):
        requests.append(request)
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda **kw: httpx.MockTransport(handle))
    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kw: httpx.MockTransport(handle))
    consumer = agent.AsyncAgentConsumer(managed_config())
    responses.extend([httpx.ConnectError("down"), httpx.Response(200)])
    started = time.monotonic()
    try:
        await consumer.complete(Delivery("m", "q", "", 1))
    finally:
        await consumer.aclose()
    assert time.monotonic() - started >= 0.09
    assert len(requests) == 2
