"""Queue transports: direct SQS, the Laravel Cloud agent, and Redis/Valkey."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

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
    "ConsumerFactory",
    "Delivery",
    "OutgoingMessage",
    "Producer",
    "SentMessage",
    "create_backend",
]


class ConsumerFactory(Protocol):
    def __call__(self, *, lease_seconds: int = 60) -> Consumer: ...


@dataclass(frozen=True)
class Backend:
    mode: Mode
    producer: Producer
    consumer_factory: ConsumerFactory
    """Creates the worker's consumer lazily (web processes never open one)."""


def create_backend(config: QueueConfig) -> Backend:
    """Build the producer now and defer consumer creation until worker startup."""
    from ..errors import ConfigurationError

    if config.mode == "redis":
        from .redis import RedisConsumer, RedisProducer

        redis_config = config.redis
        if redis_config is None:
            raise ConfigurationError("Redis backend requires Redis configuration.")

        def redis_consumer(*, lease_seconds: int = 60) -> Consumer:
            return RedisConsumer(redis_config, lease_seconds=lease_seconds)

        return Backend(config.mode, RedisProducer(redis_config), redis_consumer)

    from .sqs import SqsConsumer, SqsProducer

    connection = config.sqs
    if config.mode not in ("managed", "sqs") or connection is None:
        raise ConfigurationError("SQS backend requires SQS configuration.")
    if config.mode == "managed":
        managed = config.managed
        if managed is None:
            raise ConfigurationError("Managed backend requires managed configuration.")
        if managed.agent.enabled:
            from .agent import AgentConsumer

            def agent_consumer(*, lease_seconds: int = 60) -> Consumer:
                return AgentConsumer(managed)

            return Backend(config.mode, SqsProducer(connection), agent_consumer)

    def sqs_consumer(*, lease_seconds: int = 60) -> Consumer:
        return SqsConsumer(connection, lease_seconds=lease_seconds)

    return Backend(config.mode, SqsProducer(connection), sqs_consumer)
