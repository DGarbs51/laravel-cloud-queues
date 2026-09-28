"""The queue transports for direct SQS, the Laravel Cloud agent, and Redis or Valkey."""

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
    """A callable that creates the worker's consumer."""

    def __call__(self, *, lease_seconds: int = 60) -> Consumer:
        """Create a new consumer with the given lease duration."""
        ...


@dataclass(frozen=True)
class Backend:
    """The producer and consumer factory for a configured queue mode."""

    mode: Mode
    """The queue mode the backend was built for."""
    producer: Producer
    """The producer used to send messages."""
    consumer_factory: ConsumerFactory
    """The factory that lazily creates the worker's consumer.

    Web processes never open a consumer.
    """


def create_backend(config: QueueConfig) -> Backend:
    """Create the queue backend for the given configuration.

    The producer is built immediately, while consumer creation is deferred until the
    worker starts. Raises a ``ConfigurationError`` if the settings for the mode are missing.
    """
    from ..errors import ConfigurationError

    if config.mode == "redis":
        from .redis import RedisConsumer, RedisProducer

        redis_config = config.redis
        if redis_config is None:
            raise ConfigurationError("Redis backend requires Redis configuration.")

        def redis_consumer(*, lease_seconds: int = 60) -> Consumer:
            """Create a new Redis consumer."""
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
                """Create a new agent consumer, which ignores the lease duration."""
                return AgentConsumer(managed)

            return Backend(config.mode, SqsProducer(connection), agent_consumer)

    def sqs_consumer(*, lease_seconds: int = 60) -> Consumer:
        """Create a new SQS consumer."""
        return SqsConsumer(connection, lease_seconds=lease_seconds)

    return Backend(config.mode, SqsProducer(connection), sqs_consumer)
