"""Real UDS parity: symfony-on-cloud/tests/AgentSocketIntegrationTest.php:52."""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from harness.agent_emulator import DEFAULT_QUEUE_URL, status

from laravel_cloud_queues.config import AgentConfig, ManagedQueuesConfig, SqsConnectionConfig
from laravel_cloud_queues.errors import AgentProtocolError, AgentUnavailableError
from laravel_cloud_queues.transports import agent

pytestmark = pytest.mark.agent
FIXTURES = Path(__file__).parents[2] / "fixtures" / "agent"
FAULTS = json.loads((FIXTURES / "faults.json").read_text())
RESPONSES = json.loads((FIXTURES / "responses.json").read_text())


@pytest.fixture
def consumer(agent_emulator, monkeypatch):
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
    client = agent.AgentConsumer(
        ManagedQueuesConfig(
            connection=connection,
            agent=AgentConfig(enabled=True, socket=agent_emulator.socket_path),
            queue="assigned",
        )
    )
    try:
        yield client
    finally:
        client.close()


def wait_until(predicate):
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline, "Timed out waiting for emulator state"
        time.sleep(0.005)


def test_delivery_processed_deletes(agent_emulator, consumer):
    """Queue.php:260,339; CloudJob.php:39: agent owns completion, body opaque."""
    message_id = agent_emulator.enqueue("opaque payload")
    delivery = consumer.receive(["ignored"], 0)
    assert delivery.message_id == message_id
    assert delivery.receipt == agent_emulator.message(message_id).receipt_handle
    assert delivery.body == "opaque payload"
    assert delivery.attempt == 1
    assert delivery.queue == "default"
    assert delivery.meta == {"queue_url": DEFAULT_QUEUE_URL}
    assert consumer.renew(delivery, 100) is None
    consumer.complete(delivery)
    assert agent_emulator.message(message_id).status == "processed"
    assert consumer.receive([], 0) is None
    assert agent_emulator.results[0].body == {
        "messageId": message_id,
        "receiptHandle": delivery.receipt,
        "status": "processed",
    }


def test_visibility_expiry_and_stale_receipt(agent_emulator, consumer):
    """Queue.php:278 (counts), :349 (4xx); expiry/404 code is emulator policy."""
    agent_emulator.visibility_timeout = 0.04
    message_id = agent_emulator.enqueue("payload")
    old = consumer.receive([], 0)
    wait_until(lambda: agent_emulator.message(message_id).status == "pending")
    agent_emulator.visibility_timeout = 10
    new = consumer.receive([], 0)
    assert new.message_id == old.message_id
    assert new.receipt != old.receipt
    assert new.attempt == 2
    with pytest.raises(AgentProtocolError) as exc:
        consumer.complete(old)
    assert exc.value.status == 404
    with pytest.raises(RuntimeError):
        consumer.release(old, 0)
    consumer.complete(new)
    assert len(agent_emulator.results) == 2


@pytest.mark.parametrize("delay", [0, 1])
def test_release_redelivers_same_message(agent_emulator, consumer, delay):
    """CloudJob.php:57; Queue.php:339-344: released carries delay including zero."""
    message_id = agent_emulator.enqueue("payload")
    first = consumer.receive([], 0)
    before = time.monotonic()
    consumer.release(first, delay)
    assert agent_emulator.results[0].body["delay"] == delay
    if delay:
        assert consumer.receive([], 0) is None
        time.sleep(max(0, before + delay + 0.02 - time.monotonic()))
    second = consumer.receive([], 0)
    assert second.message_id == message_id
    assert second.attempt == 2
    assert second.receipt != first.receipt
    consumer.complete(second)


@pytest.mark.parametrize("case", FAULTS, ids=lambda c: c["name"])
def test_protocol_fault_fixtures(agent_emulator, consumer, case):
    """Queue.php:294-356 / D13.3; fixture README gives per-case source citations."""
    delivery = None
    if case["endpoint"] == "result":
        agent_emulator.enqueue("payload")
        delivery = consumer.receive([], 0)
    agent_emulator.inject(case["endpoint"], status(case["status"], case["raw"]), times=3)
    error = AgentProtocolError if case["error"] == "AgentProtocolError" else AgentUnavailableError
    with pytest.raises(error):
        consumer.receive([], 0) if delivery is None else consumer.complete(delivery)
    assert len(agent_emulator.faults_fired) == case["attempts"]
    if delivery is not None:
        with pytest.raises(RuntimeError):
            consumer.release(delivery, 0)
        assert len(agent_emulator.results) == 1


@pytest.mark.parametrize("case", RESPONSES, ids=lambda c: c["name"])
def test_protocol_response_fixtures(agent_emulator, consumer, case):
    """Queue.php:264-281; shared public-protocol fixtures also drive MockTransport."""
    body = case["raw"] if "raw" in case else json.dumps(case["body"])
    agent_emulator.inject("next", status(case["status"], body))
    delivery = consumer.receive([], 0)
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
def test_named_emulator_faults(agent_emulator, consumer, fault):
    agent_emulator.inject("next", fault)
    if fault in {"malformed_json", "non_object_json"}:
        with pytest.raises(AgentUnavailableError):
            consumer.receive([], 0)
    elif fault == "non_string_fields":
        delivery = consumer.receive([], 0)
        assert delivery.receipt is None
        assert delivery.body == ""
        assert delivery.attempt == 1
    else:
        assert consumer.receive([], 0) is None


@pytest.mark.parametrize("endpoint", ["next", "result"])
@pytest.mark.parametrize("failures", [1, 2, 3])
def test_disconnect_retries(agent_emulator, consumer, endpoint, failures):
    """Queue.php:299,338: lost connections retry; identical reports on ambiguity."""
    agent_emulator.enqueue("payload")
    delivery = consumer.receive([], 0) if endpoint == "result" else None
    agent_emulator.inject(endpoint, "disconnect", times=failures)

    def call():
        return consumer.receive([], 0) if delivery is None else consumer.complete(delivery)

    if failures == 3:
        with pytest.raises(AgentUnavailableError):
            call()
    else:
        result = call()
        if endpoint == "next":
            assert result.body == "payload"
    assert len(agent_emulator.faults_fired) == failures
    if endpoint == "result":
        assert len(agent_emulator.results) == min(3, failures + 1)
        assert len({r.raw_body for r in agent_emulator.results}) == 1


def test_apply_then_disconnect_never_sends_second_outcome(agent_emulator, consumer):
    """QueueTest.php:992 / worker acknowledgement contract; 404 is emulator policy."""
    message_id = agent_emulator.enqueue("payload")
    delivery = consumer.receive([], 0)
    agent_emulator.inject("result", "apply_then_disconnect")
    with pytest.raises(AgentProtocolError) as exc:
        consumer.complete(delivery)
    assert exc.value.status == 404
    assert agent_emulator.message(message_id).status == "processed"
    assert len(agent_emulator.results) == 2
    assert agent_emulator.results[0].applied_code == 200
    assert agent_emulator.results[0].raw_body == agent_emulator.results[1].raw_body
    with pytest.raises(RuntimeError):
        consumer.release(delivery, 0)
    assert len(agent_emulator.results) == 2


@pytest.mark.parametrize("reconnect_result", [False, True])
def test_hang_interrupt_returns_promptly(agent_emulator, consumer, reconnect_result):
    """PROJECT_SCOPE §13: wake idle GET, including a socket reconnected by POST."""
    if reconnect_result:
        agent_emulator.enqueue("payload")
        delivery = consumer.receive([], 0)
        agent_emulator.inject("result", "disconnect")
        consumer.complete(delivery)
    agent_emulator.inject("next", "hang")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(consumer.receive, [], 0)
        wait_until(lambda: any(f.kind == "hang" for _, f in agent_emulator.faults_fired))
        start = time.monotonic()
        consumer.interrupt()
        consumer.interrupt()
        try:
            assert future.result(timeout=1) is None
            assert time.monotonic() - start < 1
        finally:
            # A regression should fail promptly instead of waiting the 65s poll timeout.
            agent_emulator.stop()
    assert consumer.receive([], 0) is None


def test_interrupt_does_not_cancel_result(agent_emulator, consumer):
    """PROJECT_SCOPE §13: repeated shutdown signals must not skip reporting."""
    message_id = agent_emulator.enqueue("payload")
    delivery = consumer.receive([], 0)
    agent_emulator.inject("result", "disconnect", times=2)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(consumer.complete, delivery)
        wait_until(lambda: len(agent_emulator.faults_fired) == 1)
        consumer.interrupt()
        consumer.interrupt()
        future.result(timeout=2)
    assert agent_emulator.message(message_id).status == "processed"
    assert len(agent_emulator.results) == 3


def test_competing_outcomes_send_only_once(agent_emulator, consumer):
    delivery_id = agent_emulator.enqueue("payload")
    delivery = consumer.receive([], 0)
    barrier = threading.Barrier(2)

    def report(release):
        barrier.wait()
        try:
            if release:
                consumer.release(delivery, 0)
            else:
                consumer.complete(delivery)
            return "reported"
        except RuntimeError:
            return "blocked"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(report, [True, False])) == ["blocked", "reported"]
    assert len(agent_emulator.results) == 1
    assert agent_emulator.results[0].body["messageId"] == delivery_id
