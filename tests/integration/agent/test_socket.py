"""Real UDS parity: symfony-on-cloud/tests/AgentSocketIntegrationTest.php:52."""

import json
import threading
import time
from pathlib import Path

import anyio
import pytest

from laravel_cloud_queues.config import AgentConfig, ManagedQueuesConfig, SqsConnectionConfig
from laravel_cloud_queues.errors import AgentProtocolError, AgentUnavailableError
from laravel_cloud_queues.transports import agent
from laravel_cloud_queues.transports._threaded import ThreadedConsumer
from tests.harness.agent_emulator import DEFAULT_QUEUE_URL, delay, status

pytestmark = [pytest.mark.agent, pytest.mark.anyio]
FIXTURES = Path(__file__).parents[2] / "fixtures" / "agent"
FAULTS = json.loads((FIXTURES / "faults.json").read_text())
RESPONSES = json.loads((FIXTURES / "responses.json").read_text())


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(params=["threaded", "async"])
async def consumer(request, agent_emulator, monkeypatch, anyio_backend):
    del anyio_backend
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    agent_emulator.poll_wait = 0.02
    connection = SqsConnectionConfig(
        prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
        region="us-east-1",
        credentials="ecs",
    )

    # Narrow double until L3a's normalize_queue lands; assert delegation, don't
    # implement another lane's normalization algorithm in these tests.
    def normalize(config, url):
        assert config is connection
        assert url in (DEFAULT_QUEUE_URL, f"{connection.prefix}/emails-prod")
        return "default" if url == DEFAULT_QUEUE_URL else "emails"

    monkeypatch.setattr(agent, "normalize_queue", normalize)
    managed = ManagedQueuesConfig(
        connection=connection,
        agent=AgentConfig(enabled=True, socket=agent_emulator.socket_path),
        queue="assigned",
    )
    client = (
        ThreadedConsumer(agent.AgentConsumer(managed))
        if request.param == "threaded"
        else agent.AsyncAgentConsumer(managed)
    )
    try:
        yield client
    finally:
        await client.aclose()


def wait_until(predicate):
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline, "Timed out waiting for emulator state"
        time.sleep(0.005)


async def test_delivery_processed_deletes(agent_emulator, consumer):
    """Queue.php:260,339; CloudJob.php:39: agent owns completion, body opaque."""
    message_id = agent_emulator.enqueue("opaque payload")
    delivery = await consumer.receive(["ignored"], 0)
    assert delivery.message_id == message_id
    assert delivery.receipt == agent_emulator.message(message_id).receipt_handle
    assert delivery.body == "opaque payload"
    assert delivery.attempt == 1
    assert delivery.queue == "default"
    assert delivery.meta == {"queue_url": DEFAULT_QUEUE_URL}
    assert await consumer.renew(delivery, 100) is None
    await consumer.complete(delivery)
    assert agent_emulator.message(message_id).status == "processed"
    assert await consumer.receive([], 0) is None
    assert agent_emulator.results[0].body == {
        "messageId": message_id,
        "receiptHandle": delivery.receipt,
        "status": "processed",
    }


async def test_visibility_expiry_and_stale_receipt(agent_emulator, consumer):
    """Queue.php:278 (counts), :349 (4xx); expiry/404 code is emulator policy."""
    agent_emulator.visibility_timeout = 0.04
    message_id = agent_emulator.enqueue("payload")
    old = await consumer.receive([], 0)
    await anyio.to_thread.run_sync(
        wait_until, lambda: agent_emulator.message(message_id).status == "pending"
    )
    agent_emulator.visibility_timeout = 10
    new = await consumer.receive([], 0)
    assert new.message_id == old.message_id
    assert new.receipt != old.receipt
    assert new.attempt == 2
    with pytest.raises(AgentProtocolError) as exc:
        await consumer.complete(old)
    assert exc.value.status == 404
    with pytest.raises(RuntimeError):
        await consumer.release(old, 0)
    await consumer.complete(new)
    assert len(agent_emulator.results) == 2


@pytest.mark.parametrize("delay_seconds", [0, 1])
async def test_release_redelivers_same_message(agent_emulator, consumer, delay_seconds):
    """CloudJob.php:57; Queue.php:339-344: released carries delay including zero."""
    message_id = agent_emulator.enqueue("payload")
    first = await consumer.receive([], 0)
    before = time.monotonic()
    await consumer.release(first, delay_seconds)
    assert agent_emulator.results[0].body["delay"] == delay_seconds
    if delay_seconds:
        assert await consumer.receive([], 0) is None
        await anyio.sleep(max(0, before + delay_seconds + 0.02 - time.monotonic()))
    second = await consumer.receive([], 0)
    assert second.message_id == message_id
    assert second.attempt == 2
    assert second.receipt != first.receipt
    await consumer.complete(second)


@pytest.mark.parametrize("case", FAULTS, ids=lambda c: c["name"])
async def test_protocol_fault_fixtures(agent_emulator, consumer, case):
    """Queue.php:294-356 / D13.3; fixture README gives per-case source citations."""
    delivery = None
    if case["endpoint"] == "result":
        agent_emulator.enqueue("payload")
        delivery = await consumer.receive([], 0)
    agent_emulator.inject(case["endpoint"], status(case["status"], case["raw"]), times=3)
    error = AgentProtocolError if case["error"] == "AgentProtocolError" else AgentUnavailableError
    with pytest.raises(error):
        await consumer.receive([], 0) if delivery is None else await consumer.complete(delivery)
    assert len(agent_emulator.faults_fired) == case["attempts"]
    if delivery is not None:
        with pytest.raises(RuntimeError):
            await consumer.release(delivery, 0)
        assert len(agent_emulator.results) == 1


@pytest.mark.parametrize("case", RESPONSES, ids=lambda c: c["name"])
async def test_protocol_response_fixtures(agent_emulator, consumer, case):
    """Queue.php:264-281; shared public-protocol fixtures also drive MockTransport."""
    body = case["raw"] if "raw" in case else json.dumps(case["body"])
    agent_emulator.inject("next", status(case["status"], body))
    delivery = await consumer.receive([], 0)
    if case["expected"] is None:
        assert delivery is None
    else:
        for key, value in case["expected"].items():
            assert getattr(delivery, key) == value
    assert len(agent_emulator.faults_fired) == 1


@pytest.mark.parametrize(
    "fault",
    [
        "malformed_json",
        "non_object_json",
        "missing_message_id",
        "empty_message_id",
        "non_string_fields",
    ],
)
async def test_named_emulator_faults(agent_emulator, consumer, fault):
    agent_emulator.inject("next", fault)
    if fault in {"malformed_json", "non_object_json"}:
        with pytest.raises(AgentUnavailableError):
            await consumer.receive([], 0)
    elif fault == "non_string_fields":
        delivery = await consumer.receive([], 0)
        assert delivery.receipt is None
        assert delivery.body == ""
        assert delivery.attempt == 1
    else:
        assert await consumer.receive([], 0) is None


@pytest.mark.parametrize("endpoint", ["next", "result"])
@pytest.mark.parametrize("failures", [1, 2, 3])
async def test_disconnect_retries(agent_emulator, consumer, endpoint, failures):
    """Queue.php:299,338: lost connections retry; identical reports on ambiguity."""
    agent_emulator.enqueue("payload")
    delivery = await consumer.receive([], 0) if endpoint == "result" else None
    agent_emulator.inject(endpoint, "disconnect", times=failures)

    if failures == 3:
        with pytest.raises(AgentUnavailableError):
            await consumer.receive([], 0) if delivery is None else await consumer.complete(delivery)
    else:
        result = (
            await consumer.receive([], 0) if delivery is None else await consumer.complete(delivery)
        )
        if endpoint == "next":
            assert result.body == "payload"
    assert len(agent_emulator.faults_fired) == failures
    if endpoint == "result":
        assert len(agent_emulator.results) == min(3, failures + 1)
        assert len({r.raw_body for r in agent_emulator.results}) == 1


async def test_apply_then_disconnect_never_sends_second_outcome(agent_emulator, consumer):
    """QueueTest.php:992 / worker acknowledgement contract; 404 is emulator policy."""
    message_id = agent_emulator.enqueue("payload")
    delivery = await consumer.receive([], 0)
    agent_emulator.inject("result", "apply_then_disconnect")
    with pytest.raises(AgentProtocolError) as exc:
        await consumer.complete(delivery)
    assert exc.value.status == 404
    assert agent_emulator.message(message_id).status == "processed"
    assert len(agent_emulator.results) == 2
    assert agent_emulator.results[0].applied_code == 200
    assert agent_emulator.results[0].raw_body == agent_emulator.results[1].raw_body
    with pytest.raises(RuntimeError):
        await consumer.release(delivery, 0)
    assert len(agent_emulator.results) == 2


async def test_interrupt_waits_for_the_in_flight_poll_and_keeps_the_handover(
    agent_emulator, consumer
):
    """Laravel parity: a stop signal during an idle GET /next never
    aborts the poll. The message the agent hands over afterwards is returned, and only
    the next receive returns None promptly."""
    message_id = agent_emulator.enqueue("payload")
    agent_emulator.inject("next", delay(0.3))
    times = {}

    def stop_once_in_flight():
        wait_until(lambda: any(f.kind == "delay" for _, f in agent_emulator.faults_fired))
        times["start"] = time.monotonic()
        consumer.interrupt()
        consumer.interrupt()

    async with anyio.create_task_group() as tg:
        tg.start_soon(anyio.to_thread.run_sync, stop_once_in_flight)
        delivery = await consumer.receive([], 0)
    assert delivery is not None and delivery.message_id == message_id
    assert time.monotonic() - times["start"] >= 0.2
    assert agent_emulator.message(message_id).status == "in_flight"
    start = time.monotonic()
    assert await consumer.receive([], 0) is None
    assert time.monotonic() - start < 0.5
    await consumer.complete(delivery)
    assert agent_emulator.message(message_id).status == "processed"


async def test_interrupt_does_not_cancel_result(agent_emulator, consumer):
    """Repeated shutdown signals must not skip reporting."""
    message_id = agent_emulator.enqueue("payload")
    delivery = await consumer.receive([], 0)
    agent_emulator.inject("result", "disconnect", times=2)

    def stop_once_reporting():
        wait_until(lambda: len(agent_emulator.faults_fired) == 1)
        consumer.interrupt()
        consumer.interrupt()

    async with anyio.create_task_group() as tg:
        tg.start_soon(anyio.to_thread.run_sync, stop_once_reporting)
        await consumer.complete(delivery)
    assert agent_emulator.message(message_id).status == "processed"
    assert len(agent_emulator.results) == 3


async def test_competing_outcomes_send_only_once(agent_emulator, consumer):
    delivery_id = agent_emulator.enqueue("payload")
    delivery = await consumer.receive([], 0)
    barrier = threading.Barrier(2)
    results = []

    async def report(release):
        await anyio.to_thread.run_sync(barrier.wait)
        try:
            if release:
                await consumer.release(delivery, 0)
            else:
                await consumer.complete(delivery)
            results.append("reported")
        except RuntimeError:
            results.append("blocked")

    async with anyio.create_task_group() as tg:
        tg.start_soon(report, True)
        tg.start_soon(report, False)
    assert sorted(results) == ["blocked", "reported"]
    assert len(agent_emulator.results) == 1
    assert agent_emulator.results[0].body["messageId"] == delivery_id


async def test_twin_shares_the_outcome(agent_emulator, consumer):
    agent_emulator.enqueue("payload")
    delivery = await consumer.receive([], 0)
    await consumer.complete(delivery)
    with pytest.raises(RuntimeError, match="already"):
        consumer.blocking.complete(delivery)
    assert len(agent_emulator.results) == 1
