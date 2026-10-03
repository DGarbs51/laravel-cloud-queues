"""Narrow test doubles for the dispatch seams (``Registry(backend=..., telemetry=...)``)."""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any

from laravel_cloud_queues.config import Mode, QueueConfig
from laravel_cloud_queues.observability import Telemetry
from laravel_cloud_queues.registry import Registry
from laravel_cloud_queues.transports import (
    SQS_MAX_PAYLOAD_BYTES,
    Backend,
    Consumer,
    OutgoingMessage,
    SentMessage,
)


class FakeProducer:
    def __init__(
        self,
        *,
        supports_fifo: bool = True,
        max_payload_bytes: int | None = SQS_MAX_PAYLOAD_BYTES,
        error: Exception | None = None,
    ) -> None:
        self._supports_fifo = supports_fifo
        self._max_payload_bytes = max_payload_bytes
        self.error = error
        self.sent: list[OutgoingMessage] = []
        self.threads: list[int] = []

    @property
    def max_payload_bytes(self) -> int | None:
        return self._max_payload_bytes

    @property
    def supports_fifo(self) -> bool:
        return self._supports_fifo

    def send(self, message: OutgoingMessage) -> SentMessage:
        self.threads.append(threading.get_ident())
        if self.error is not None:
            raise self.error
        self.sent.append(message)
        return SentMessage(message_id=f"msg-{len(self.sent)}", queue=message.queue)

    def close(self) -> None:
        return None


class RecordingTelemetry(Telemetry):
    def __init__(self, *, error: Exception | None = None) -> None:  # no super(): no sink
        self.events: list[dict[str, object]] = []
        self.error = error

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(dict(event))

    def log_line(self, record: Mapping[str, object], **options: object) -> None:
        return None


def _no_consumer(*, lease_seconds: int = 60) -> Consumer:
    raise AssertionError("dispatch tests never open a consumer")


def make_registry(
    *,
    mode: Mode = "sqs",
    config: QueueConfig | None = None,
    producer: FakeProducer | None = None,
    telemetry: RecordingTelemetry | None = None,
    **kwargs: Any,
) -> tuple[Registry, FakeProducer, RecordingTelemetry]:
    producer = producer or FakeProducer(
        supports_fifo=mode != "redis",
        max_payload_bytes=None if mode == "redis" else SQS_MAX_PAYLOAD_BYTES,
    )
    telemetry = telemetry or RecordingTelemetry()
    registry = Registry(
        config=config or QueueConfig(mode=mode),
        backend=Backend(mode=mode, producer=producer, consumer_factory=_no_consumer),
        telemetry=telemetry,
        **kwargs,
    )
    return registry, producer, telemetry
