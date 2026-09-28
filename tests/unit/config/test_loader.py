import json
import logging

import pytest

from laravel_cloud_queues.config import StaticCredentials, load_config
from laravel_cloud_queues.errors import ConfigurationError


def managed(**changes):
    return {
        "driver": "cloud",
        "connection": {
            "prefix": "https://sqs.example/123",
            "region": "us-east-1",
            "credentials": "ecs",
            "queue": "sender",
        },
        **changes,
    }


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"REDIS_URL": "redis://cache"},
        {"AWS_ACCESS_KEY_ID": "storage"},
        {"LARAVEL_CLOUD_QUEUES_BACKEND": "invalid"},
    ],
)
def test_backend_must_be_explicit(env):
    with pytest.raises(ConfigurationError):
        load_config(env=env)


@pytest.mark.parametrize(
    "document",
    [
        "",
        "{secret",
        "null",
        "[]",
        {},
        {"driver": "redis"},
        {"driver": "cloud"},
        managed(connection={}),
        managed(connection={"region": "us-east-1"}),
        managed(connection={"region": "us-east-1", "credentials": "default"}),
    ],
)
def test_managed_strict_shape(document):
    """CloudBootstrapper.php:223 throwing JSON; D13.4 strict-shape/explicit-only deviations."""
    with pytest.raises(ConfigurationError):
        load_config(env={}, managed_config=document)


@pytest.mark.parametrize(
    "inventory", [["emails", "orders.fifo"], {"emails": {}, "orders.fifo": {}}]
)
def test_managed_inventory_defaults_and_preservation(inventory, caplog):
    """Foundation/Cloud/Queue.php:576 inventory; CloudBootstrapper.php:225 config extras."""
    raw = managed(queues=inventory, future={"value": 1})
    raw["connection"].update(
        after_commit=True,
        overflow={"enabled": True, "secret": "hidden"},
        credential_cache={"enabled": True},
    )
    with caplog.at_level(logging.WARNING, logger="laravel_cloud_queues"):
        config = load_config(env={}, managed_config=json.dumps(raw))
    assert config.default_queue == "sender"
    assert config.managed.queue == "default"
    assert config.managed.queues == ("emails", "orders.fifo")
    assert config.managed.raw == raw
    assert config.managed.after_commit is True
    assert config.managed.overflow == raw["connection"]["overflow"]
    assert config.managed.credential_cache == {"enabled": True}
    assert "oversized payloads are rejected" in caplog.text
    assert "each worker resolves its own credentials" in caplog.text
    assert "hidden" not in caplog.text


def test_code_and_socket_precedence():
    env = {
        "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG": "invalid",
        "LARAVEL_CLOUD_AGENT_SOCKET": "/env",
        "LARAVEL_CLOUD_LOG_SOCKET": "/env-log",
    }
    config = load_config(
        env=env,
        managed_config=managed(queue="worker", agent={"enabled": True, "socket": "/config"}),
        agent_socket="/code",
        log_socket="/code-log",
        sqs_queue="code-sender",
    )
    assert config.uses_agent
    assert config.managed.queue == "worker"
    assert config.default_queue == "code-sender"
    assert config.managed.agent.socket == "/config"
    assert config.log_socket == "/code-log"
    assert (
        load_config(env=env, managed_config=managed(), agent_socket="/code").managed.agent.socket
        == "/code"
    )
    assert load_config(env=env, managed_config=managed()).managed.agent.socket == "/env"
    assert (
        load_config(env={}, managed_config=managed()).managed.agent.socket
        == "/tmp/cloud-agent.sock"
    )


@pytest.mark.parametrize("backend", ["managed", "sqs", "redis"])
def test_endpoint_forbidden_with_managed_presence(backend):
    with pytest.raises(ConfigurationError, match="endpoint"):
        load_config(
            env={
                "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG": "{}",
                "LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT": "http://localhost",
            },
            backend=backend,
        )


def test_sqs_settings_and_code_overrides():
    env = {
        f"LARAVEL_CLOUD_QUEUES_SQS_{key}": value
        for key, value in {
            "PREFIX": "https://env/123",
            "REGION": "us-west-2",
            "SUFFIX": "-env",
            "QUEUE": "env",
            "KEY": "env-key",
            "SECRET": "env-secret",
            "ENDPOINT": "http://env",
        }.items()
    }
    config = load_config(env=env, backend="sqs")
    assert config.sqs.credentials == StaticCredentials("env-key", "env-secret")
    assert config.sqs.region == "us-west-2"
    assert config.sqs.suffix == "-env"
    code = load_config(
        env=env,
        backend="sqs",
        sqs_prefix="https://code/123",
        sqs_region="us-east-1",
        sqs_suffix="",
        sqs_queue="code",
        sqs_credentials=StaticCredentials("code", "secret", "token"),
        sqs_endpoint="http://code",
    )
    assert code.default_queue == "code"
    assert code.sqs.prefix == "https://code/123"
    assert code.sqs.region == "us-east-1"
    assert code.sqs.suffix == ""
    assert code.sqs.endpoint_url == "http://code"
    assert code.sqs.credentials.token == "token"


@pytest.mark.parametrize("missing", ["PREFIX", "REGION", "KEY", "SECRET"])
def test_required_sqs_settings(missing):
    env = {
        f"LARAVEL_CLOUD_QUEUES_SQS_{key}": "value"
        for key in ["PREFIX", "REGION", "KEY", "SECRET"]
        if key != missing
    }
    with pytest.raises(ConfigurationError):
        load_config(env=env, backend="sqs")


def test_default_credentials_are_opt_in():
    config = load_config(
        env={"LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS": "default"},
        backend="sqs",
        sqs_prefix="https://example/123",
        sqs_region="us-east-1",
    )
    assert config.sqs.credentials == "default"
    with pytest.raises(ConfigurationError):
        load_config(env={"LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS": "unknown"}, backend="sqs")


def test_redis_precedence():
    env = {
        "LARAVEL_CLOUD_QUEUES_BACKEND": "sqs",
        "REDIS_URL": "redis://fallback",
        "LARAVEL_CLOUD_QUEUES_REDIS_URL": "redis://env",
    }
    assert load_config(env=env, backend="redis").redis.url == "redis://env"
    assert (
        load_config(env={"REDIS_URL": "redis://fallback"}, backend="redis").redis.url
        == "redis://fallback"
    )
    config = load_config(
        env=env, backend="redis", redis_url="redis://code", redis_queue="code", redis_prefix=""
    )
    assert (config.redis.url, config.redis.queue, config.redis.prefix) == (
        "redis://code",
        "code",
        "",
    )
    with pytest.raises(ConfigurationError):
        load_config(env={}, backend="redis")


@pytest.mark.parametrize(
    "changes",
    [
        {"queues": "bad"},
        {"queues": [1]},
        {"agent": {"enabled": "false"}},
        {"agent": []},
        {"queue": 1},
    ],
)
def test_invalid_managed_field_types(changes):
    with pytest.raises(ConfigurationError):
        load_config(env={}, managed_config=managed(**changes))
