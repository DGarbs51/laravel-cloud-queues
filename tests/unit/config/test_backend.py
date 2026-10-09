from unittest.mock import Mock

import pytest

from laravel_cloud_queues.config import (
    AgentConfig,
    ManagedQueuesConfig,
    QueueConfig,
    RedisConfig,
    SqsConnectionConfig,
)
from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.transports import create_backend
from laravel_cloud_queues.transports.sqs import SqsConsumer, SqsProducer


@pytest.mark.parametrize("mode", ["sqs", "managed"])
def test_direct_backend_defers_client(mode):
    connection = SqsConnectionConfig("prefix", "us-east-1", "ecs")
    managed = ManagedQueuesConfig(connection, AgentConfig(False)) if mode == "managed" else None
    backend = create_backend(QueueConfig(mode, sqs=connection, managed=managed))
    assert isinstance(backend.producer, SqsProducer)
    assert backend.producer._client is None
    consumer = backend.consumer_factory(lease_seconds=12)
    assert isinstance(consumer, SqsConsumer)
    assert consumer._lease_seconds == 12
    assert consumer._client is None
    assert backend.consumer_factory()._lease_seconds == 60
    # No native async SQS client: async callers get the threaded fallback.
    assert backend.async_producer_factory is None
    assert backend.async_consumer_factory is None


def test_managed_agent_factory_is_lazy(monkeypatch):
    from laravel_cloud_queues.transports import agent

    constructor, async_constructor = Mock(), Mock()
    monkeypatch.setattr(agent, "AgentConsumer", constructor)
    monkeypatch.setattr(agent, "AsyncAgentConsumer", async_constructor)
    connection = SqsConnectionConfig("prefix", "us-east-1", "ecs")
    managed = ManagedQueuesConfig(connection, AgentConfig(True))
    backend = create_backend(QueueConfig("managed", sqs=connection, managed=managed))
    constructor.assert_not_called()
    async_constructor.assert_not_called()
    assert isinstance(backend.producer, SqsProducer)
    assert backend.consumer_factory(lease_seconds=12) is constructor.return_value
    constructor.assert_called_once_with(managed)
    assert backend.open_async_consumer(lease_seconds=12) is async_constructor.return_value
    async_constructor.assert_called_once_with(managed)
    # Managed dispatch goes through SQS, which has no native async client.
    assert backend.async_producer_factory is None


def test_redis_factory_is_lazy(monkeypatch):
    from laravel_cloud_queues.transports import redis

    producer, consumer = Mock(), Mock()
    async_producer, async_consumer = Mock(), Mock()
    monkeypatch.setattr(redis, "RedisProducer", producer)
    monkeypatch.setattr(redis, "RedisConsumer", consumer)
    monkeypatch.setattr(redis, "AsyncRedisProducer", async_producer)
    monkeypatch.setattr(redis, "AsyncRedisConsumer", async_consumer)
    config = RedisConfig("redis://localhost")
    backend = create_backend(QueueConfig("redis", redis=config))
    producer.assert_called_once_with(config)
    consumer.assert_not_called()
    async_producer.assert_not_called()
    async_consumer.assert_not_called()
    backend.consumer_factory(lease_seconds=19)
    consumer.assert_called_once_with(config, lease_seconds=19)
    assert backend.open_async_producer() is async_producer.return_value
    async_producer.assert_called_once_with(config)
    assert backend.open_async_consumer(lease_seconds=19) is async_consumer.return_value
    async_consumer.assert_called_once_with(config, lease_seconds=19)


@pytest.mark.parametrize("mode", ["redis", "managed", "sqs"])
def test_missing_backend_section_fails(mode):
    with pytest.raises(ConfigurationError):
        create_backend(QueueConfig(mode))
