"""Queue transports: direct SQS, the Laravel Cloud agent, and Redis/Valkey."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ..config import Mode, QueueConfig
from .base import (
    MAX_FRESH_DELAY_SECONDS,
    MAX_VISIBILITY_SECONDS,
    SQS_MAX_PAYLOAD_BYTES,
    Consumer,
    Delivery,
    OutgoingMessage,
    Producer,
    SentMessage,
)

__all__ = [
    "MAX_FRESH_DELAY_SECONDS",
    "MAX_VISIBILITY_SECONDS",
    "SQS_MAX_PAYLOAD_BYTES",
    "Backend",
    "Consumer",
    "Delivery",
    "OutgoingMessage",
    "Producer",
    "SentMessage",
    "create_backend",
]


@dataclass(frozen=True)
class Backend:
    mode: Mode
    producer: Producer
    consumer_factory: Callable[[], Consumer]
    """Creates the worker's consumer lazily (web processes never open one)."""


def create_backend(config: QueueConfig) -> Backend:
    """Build producer/consumer for ``config.mode``. Imports ``redis`` lazily (optional extra).

    managed + agent enabled: SqsProducer + AgentConsumer. managed + agent disabled, or sqs:
    SqsProducer + SqsConsumer. redis: RedisProducer + RedisConsumer.
    CONTRACT STUB — wired by lane L3a (SQS/managed) with L4 (agent) and L5 (redis).
    """
    raise NotImplementedError
