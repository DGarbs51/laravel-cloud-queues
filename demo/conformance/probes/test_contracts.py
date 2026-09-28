"""Small public-API probes for pure contract rules not requiring a worker process.

Laravel v13.33.0: Queue/Worker.php:701-740,817-827; Queue/SqsQueue.php:579-635,687-712;
Foundation/Cloud/QueueConnector.php:29-74. Project deviations are catalogued under D13.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

import anyio
import pytest

from laravel_cloud_queues.config import (
    QueueConfig,
    SqsConnectionConfig,
    StaticCredentials,
    load_config,
)
from laravel_cloud_queues.errors import (
    ConfigurationError,
    InvalidQueueOptionError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
)
from laravel_cloud_queues.jobs.policy import RetryPolicy, WorkerDefaults, normalize_delay
from laravel_cloud_queues.registry import Registry
from laravel_cloud_queues.transports.sqs import normalize_queue, queue_url

pytestmark = pytest.mark.conformance(tier="unit")


def connection():
    return SqsConnectionConfig(
        prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
        region="us-east-1",
        credentials=StaticCredentials("test", "test"),
    )


def configured_registry():
    return Registry(config=QueueConfig(mode="sqs", sqs=connection()))


def managed():
    return {
        "driver": "cloud",
        "queue": "assigned",
        "connection": {
            "region": "us-east-1",
            "prefix": connection().prefix,
            "queue": "sender",
            "credentials": "ecs",
        },
        "agent": {"enabled": True},
    }


def test_config(evidence):
    for env in (
        {},
        {"REDIS_URL": "redis://localhost/15"},
        {"AWS_REGION": "us-east-1"},
        {"LARAVEL_CLOUD_QUEUES_BACKEND": "unknown"},
    ):
        with pytest.raises(ConfigurationError):
            load_config(env=env)
    config = load_config(env={}, managed_config=managed())
    assert config.mode == "managed"
    assert config.managed.queue == "assigned"
    assert config.default_queue == "sender"
    config = load_config(env={"REDIS_URL": "redis://localhost/15"}, backend="redis")
    assert config.mode == "redis"
    assert config.redis.url == "redis://localhost/15"
    for value in ("{", {}, {"driver": "redis"}, {"driver": "cloud"}):
        with pytest.raises(ConfigurationError):
            load_config(env={}, managed_config=value)
    for credentials in (None, "default", "unknown"):
        value = managed()
        if credentials is None:
            del value["connection"]["credentials"]
        else:
            value["connection"]["credentials"] = credentials
        with pytest.raises(ConfigurationError):
            load_config(env={}, managed_config=value)
    value = managed()
    value["queues"] = {"known": {}, "other": {}}
    value["future"] = {"preserved": True}
    config = load_config(env={}, managed_config=value)
    assert config.managed.queues == ("known", "other")
    assert config.managed.raw["future"] == {"preserved": True}
    for provider in ("ecs", "instance"):
        value["connection"]["credentials"] = provider
        assert load_config(env={}, managed_config=value).sqs.credentials == provider
    with pytest.raises(ConfigurationError):
        load_config(env={}, backend="sqs", sqs_prefix=connection().prefix, sqs_region="us-east-1")
    with pytest.raises(ConfigurationError):
        load_config(env={}, managed_config=managed(), sqs_endpoint="http://localhost:4566")
    evidence.record("providers", ["ecs", "instance"])


def test_agent_socket_precedence(evidence):
    value = managed()
    config = load_config(env={}, managed_config=value)
    assert config.managed.agent.socket == "/tmp/cloud-agent.sock"
    config = load_config(env={"LARAVEL_CLOUD_AGENT_SOCKET": "/tmp/env.sock"}, managed_config=value)
    assert config.managed.agent.socket == "/tmp/env.sock"
    value["agent"]["socket"] = "/tmp/config.sock"
    config = load_config(env={"LARAVEL_CLOUD_AGENT_SOCKET": "/tmp/env.sock"}, managed_config=value)
    assert config.managed.agent.socket == "/tmp/config.sock"
    evidence.observed("Config socket wins over environment, then platform default")


@pytest.mark.parametrize(
    ("name", "suffix", "physical", "normalized"),
    [
        ("mail", "-prod", "mail-prod", "mail"),
        ("mail-prod", "-prod", "mail-prod", "mail"),
        ("mail-prod-prod", "-prod", "mail-prod", "mail"),
        ("mail.fifo", "-prod", "mail-prod.fifo", "mail.fifo"),
        ("mail-prod.fifo", "-prod", "mail-prod.fifo", "mail.fifo"),
        ("mail", "", "mail", "mail"),
    ],
)
def test_queue_names(name, suffix, physical, normalized, evidence):
    config = replace(connection(), prefix=connection().prefix + "///", suffix=suffix)
    url = queue_url(config, name)
    assert url == connection().prefix + "/" + physical
    assert queue_url(config, url) == url
    assert normalize_queue(config, url) == normalized
    evidence.record("mapping", {"input": name, "url": url, "normalized": normalized})


def test_policy(evidence):
    default = RetryPolicy().resolve(WorkerDefaults())
    assert (default.tries, default.timeout, default.retry_delay(1)) == (1, 60, 0)
    policy = RetryPolicy(tries=3, backoff=[0.1, 2, 50000]).resolve(WorkerDefaults())
    assert [policy.retry_delay(n) for n in range(1, 5)] == [1, 2, 43200, 43200]
    assert not policy.exceeded_before_run(3)
    assert policy.exceeded_before_run(4)
    assert policy.is_last_attempt(3)
    assert not policy.is_last_attempt(2)
    unlimited = RetryPolicy(tries=0, timeout=0).resolve(WorkerDefaults())
    assert unlimited.timeout == 0
    assert not unlimited.is_last_attempt(100000)
    assert not unlimited.exceeded_before_run(100000)
    for delay in (-1, 901, float("inf"), float("nan")):
        with pytest.raises(InvalidQueueOptionError):
            normalize_delay(delay)
    assert [normalize_delay(d) for d in (0, 0.1, 900)] == [0, 1, 900]
    evidence.record("backoff", [policy.retry_delay(n) for n in range(1, 5)])


def test_declaration_builder_eager(evidence):
    registry = configured_registry()
    calls = []

    @registry.job(name="collision")
    def job(queue: str, delay: int, timeout: int) -> str:
        calls.append((queue, delay, timeout))
        return queue

    assert job("direct", 1, 2) == "direct"
    configured = job.options(queue="priority", delay=1)
    assert job.dispatch_options.queue is None
    assert configured.dispatch_options.queue == "priority"
    with registry.testing(eager=True) as recorder:
        job.dispatch(queue="payload", delay=2, timeout=3)
        anyio.run(job.dispatch_async, "async", 3, 4)
    assert calls == [("direct", 1, 2), ("payload", 2, 3), ("async", 3, 4)]
    assert recorder is not None
    with pytest.raises(ConfigurationError):
        registry.job(name="collision")(lambda: None)
    evidence.record("calls", calls)


@pytest.mark.parametrize(
    ("queue", "options"),
    [
        ("jobs.fifo", {"delay": 1}),
        ("jobs.fifo", {"message_group": "tenant"}),
        ("jobs", {"group": "group"}),
        ("jobs", {"deduplication_id": "dedup"}),
    ],
)
def test_invalid_options(queue, options):
    registry = configured_registry()

    @registry.job
    def job() -> None:
        pass

    with pytest.raises(InvalidQueueOptionError):
        job.options(queue=queue, **options).dispatch()


def test_oversized_payload():
    registry = configured_registry()

    @registry.job
    def job(value: str) -> None:
        pass

    with pytest.raises(PayloadTooLargeError):
        job.dispatch("x" * 1_048_576)


@pytest.mark.sqs
@pytest.mark.conformance("sqs.queue_not_found", tier="emulated")
def test_broker_errors(sqs_endpoint, evidence):
    from laravel_cloud_queues.transports.base import OutgoingMessage
    from laravel_cloud_queues.transports.sqs import SqsProducer

    url = sqs_endpoint.create_queue()
    prefix, name = url.rsplit("/", 1)
    producer = SqsProducer(
        SqsConnectionConfig(
            prefix=prefix,
            region=sqs_endpoint.region,
            credentials=StaticCredentials(sqs_endpoint.access_key, sqs_endpoint.secret_key),
            endpoint_url=sqs_endpoint.url,
        )
    )
    try:
        with pytest.raises(ManagedQueueNotFoundError):
            producer.send(OutgoingMessage("{}", name + "-missing"))
        sqs_endpoint.client.set_queue_attributes(
            QueueUrl=url, Attributes={"MaximumMessageSize": "1024"}
        )
        with pytest.raises(PayloadTooLargeError):
            producer.send(OutgoingMessage("x" * 2048, name))
    finally:
        producer.close()
    evidence.observed("Actual emulator errors translated to package error types")


@pytest.mark.socket
@pytest.mark.conformance("observability.failed_job_size_policy", tier="socket")
def test_failure_sizes(log_collector, evidence):
    from laravel_cloud_queues.observability import (
        SocketEventSink,
        encode_event_line,
        failed_job_event,
    )

    now = datetime.now(timezone.utc)
    sink = SocketEventSink(log_collector.socket_path)
    try:
        for payload_size, error_size, replayable in [
            (1, 1, True),
            (1, 30000, True),
            (30000, 30000, False),
        ]:
            payload = json.dumps({"displayName": "demo.payload", "value": "é" * payload_size})
            event = failed_job_event(
                queue="default",
                payload=payload,
                exception=RuntimeError("é" * error_size),
                attempts=1,
                started_at=now,
                timestamp=now,
            )
            assert len(encode_event_line(event)) <= 16384
            assert event.get("replayable", True) == replayable
            if replayable:
                assert event["payload"] == payload
            assert sink.emit(event)
        events = log_collector.wait_for(lambda events: len(events) == 3)
        evidence.record("encoded_lengths", [len(encode_event_line(event)) for event in events])
        assert not log_collector.errors
    finally:
        sink.close()


@pytest.mark.socket
@pytest.mark.conformance("observability.socket_writer", tier="socket")
def test_socket_writer(log_collector, evidence):
    from concurrent.futures import ThreadPoolExecutor

    from laravel_cloud_queues.observability import SocketEventSink, lifecycle_event

    sink = SocketEventSink(log_collector.socket_path)
    event = lifecycle_event("queued", "café/queue", timestamp=datetime.now(timezone.utc))
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert all(pool.map(sink.emit, [event] * 20))
        log_collector.wait_for(lambda events: len(events) == 20)
        log_collector.close_clients()
        assert sink.emit(event)
        log_collector.wait_for(lambda events: len(events) == 21)
        assert not log_collector.errors
        assert all(
            line.endswith(b"\n") and b"caf\xc3\xa9/queue" in line
            for line in log_collector.raw_lines
        )
        evidence.record("complete_lines", len(log_collector.raw_lines))
    finally:
        sink.close()


@pytest.mark.socket
@pytest.mark.conformance("observability.queued_event", tier="socket")
def test_queued_event_after_send(log_collector, evidence):
    from laravel_cloud_queues.config import AgentConfig, ManagedQueuesConfig
    from laravel_cloud_queues.observability import SocketEventSink, Telemetry
    from laravel_cloud_queues.transports import Backend
    from laravel_cloud_queues.transports.base import SentMessage

    class Producer:
        max_payload_bytes = 1048576
        supports_fifo = True
        fail = False

        def send(self, message):
            if self.fail:
                raise ManagedQueueNotFoundError("synthetic missing queue")
            return SentMessage("synthetic-id", message.queue)

        def close(self):
            pass

    producer = Producer()
    sink = SocketEventSink(log_collector.socket_path)
    managed_config = ManagedQueuesConfig(connection=connection(), agent=AgentConfig(enabled=True))
    # Producer success/failure is deterministic; telemetry crosses the real Unix socket.
    registry = Registry(
        config=QueueConfig(mode="managed", sqs=connection(), managed=managed_config),
        backend=Backend(mode="managed", producer=producer, consumer_factory=lambda **kw: None),
        telemetry=Telemetry(sink=sink, emits_cloud_events=True),
    )

    @registry.job
    def job() -> None:
        pass

    try:
        job.dispatch()
        events = log_collector.wait_for(lambda events: len(events) == 1)
        assert events[0]["type"] == "queued"
        assert "duration_ms" not in events[0]
        producer.fail = True
        with pytest.raises(ManagedQueueNotFoundError):
            job.dispatch()
        assert len(log_collector.events) == 1
        evidence.record("events", events)
    finally:
        sink.close()
