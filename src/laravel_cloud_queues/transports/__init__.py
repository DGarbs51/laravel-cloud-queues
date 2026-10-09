"""The queue transports for direct SQS, the Laravel Cloud agent, and Redis or Valkey."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ..config import Mode, QueueConfig
from .base import (
    MAX_FRESH_DELAY_SECONDS,
    MAX_VISIBILITY_SECONDS,
    SQS_MAX_PAYLOAD_BYTES,
    AsyncConsumer,
    AsyncProducer,
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
    "AsyncConsumer",
    "AsyncConsumerFactory",
    "AsyncProducer",
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


class AsyncConsumerFactory(Protocol):
    """A callable that creates the worker's async consumer."""

    def __call__(self, *, lease_seconds: int = 60) -> AsyncConsumer:
        """Create a new async consumer with the given lease duration."""
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
    async_producer_factory: Callable[[], AsyncProducer] | None = None
    """The factory that creates a native async producer for the running event loop.

    When ``None``, async dispatch sends through :attr:`producer` in a worker thread.
    """
    async_consumer_factory: AsyncConsumerFactory | None = None
    """The factory that creates the worker's native async consumer.

    When ``None``, the worker runs a consumer from :attr:`consumer_factory` in a worker
    thread.
    """

    def open_async_producer(self) -> AsyncProducer:
        """Create a new async producer, which belongs to the running event loop.

        This never performs I/O, so it is safe to call on the loop.
        """
        if self.async_producer_factory is None:
            from ._threaded import ThreadedProducer

            return ThreadedProducer(self.producer)
        return self.async_producer_factory()

    def open_async_consumer(self, *, lease_seconds: int = 60) -> AsyncConsumer:
        """Create a new async consumer with the given lease duration."""
        if self.async_consumer_factory is None:
            from ._threaded import ThreadedConsumer

            return ThreadedConsumer(self.consumer_factory(lease_seconds=lease_seconds))
        return self.async_consumer_factory(lease_seconds=lease_seconds)


def create_backend(config: QueueConfig) -> Backend:
    """Create the queue backend for the given configuration.

    The producer is built immediately, while consumer creation is deferred until the
    worker starts. Raises a ``ConfigurationError`` if the settings for the mode are missing.
    """
    from ..errors import ConfigurationError

    if config.mode == "redis":
        from .redis import AsyncRedisConsumer, AsyncRedisProducer, RedisConsumer, RedisProducer

        redis_config = config.redis
        if redis_config is None:
            raise ConfigurationError("Redis backend requires Redis configuration.")

        def redis_consumer(*, lease_seconds: int = 60) -> Consumer:
            """Create a new Redis consumer."""
            return RedisConsumer(redis_config, lease_seconds=lease_seconds)

        def async_redis_consumer(*, lease_seconds: int = 60) -> AsyncConsumer:
            """Create a new native async Redis consumer."""
            return AsyncRedisConsumer(redis_config, lease_seconds=lease_seconds)

        return Backend(
            config.mode,
            RedisProducer(redis_config),
            redis_consumer,
            async_producer_factory=lambda: AsyncRedisProducer(redis_config),
            async_consumer_factory=async_redis_consumer,
        )

    from .sqs import SqsConsumer, SqsProducer

    connection = config.sqs
    if config.mode not in ("managed", "sqs") or connection is None:
        raise ConfigurationError("SQS backend requires SQS configuration.")
    if config.mode == "managed":
        managed = config.managed
        if managed is None:
            raise ConfigurationError("Managed backend requires managed configuration.")
        if managed.agent.enabled:
            from .agent import AgentConsumer, AsyncAgentConsumer

            def agent_consumer(*, lease_seconds: int = 60) -> Consumer:
                """Create a new agent consumer, which ignores the lease duration."""
                return AgentConsumer(managed)

            def async_agent_consumer(*, lease_seconds: int = 60) -> AsyncConsumer:
                """Create a new native async agent consumer, which ignores the lease duration."""
                return AsyncAgentConsumer(managed)

            return Backend(
                config.mode,
                SqsProducer(connection),
                agent_consumer,
                async_consumer_factory=async_agent_consumer,
            )

    def sqs_consumer(*, lease_seconds: int = 60) -> Consumer:
        """Create a new SQS consumer."""
        return SqsConsumer(connection, lease_seconds=lease_seconds)

    return Backend(config.mode, SqsProducer(connection), sqs_consumer)
