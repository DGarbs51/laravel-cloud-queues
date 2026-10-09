"""Small public-API probes for pure contract rules not requiring a worker process.

Laravel v13.33.0: Queue/Worker.php:701-740,817-827; Queue/SqsQueue.php:579-635,687-712;
Foundation/Cloud/QueueConnector.php:29-74. Project deviations are catalogued under D13.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from laravel_cloud_queues.config import (
    QueueConfig,
    SqsConnectionConfig,
    StaticCredentials,
)
from laravel_cloud_queues.errors import (
    ManagedQueueNotFoundError,
)
from laravel_cloud_queues.registry import Registry

pytestmark = pytest.mark.conformance(tier="unit")


def connection():
    return SqsConnectionConfig(
        prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
        region="us-east-1",
        credentials=StaticCredentials("test", "test"),
    )


@pytest.mark.socket
@pytest.mark.conformance("observability.failed_job_size_policy", tier="socket")
def test_failure_sizes(log_collector, evidence):
    from laravel_cloud_queues.observability import (
        SocketEventSink,
        encode_event_line,
        failed_job_event,
    )

    now = datetime.now(UTC)
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
