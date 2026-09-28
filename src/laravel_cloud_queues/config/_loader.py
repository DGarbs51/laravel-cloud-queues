"""Resolve explicit backend settings and Laravel Cloud's managed configuration."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from typing import Any

from ..errors import ConfigurationError
from ._models import (
    DEFAULT_AGENT_SOCKET,
    DEFAULT_LOG_SOCKET,
    DEFAULT_QUEUE,
    DEFAULT_REDIS_PREFIX,
    AgentConfig,
    ManagedQueuesConfig,
    Mode,
    QueueConfig,
    RedisConfig,
    SqsConnectionConfig,
    SqsCredentials,
    StaticCredentials,
)


def _string(value: object, setting: str, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not value and not empty):
        raise ConfigurationError(f"{setting} must be a {'possibly empty ' if empty else ''}string.")
    return value


def _object(value: object, setting: str) -> dict[str, Any]:
    # Any is confined to the forward-compatible, arbitrary JSON configuration document.
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise ConfigurationError(f"{setting} must be an object.")
    return dict(value)


def _boolean(value: object, setting: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigurationError(f"{setting} must be a boolean.")
    return value


def load_config(
    *,
    env: Mapping[str, str] | None = None,
    backend: Mode | None = None,
    managed_config: str | Mapping[str, Any] | None = None,
    sqs_prefix: str | None = None,
    sqs_suffix: str | None = None,
    sqs_queue: str | None = None,
    sqs_region: str | None = None,
    sqs_credentials: SqsCredentials | None = None,
    sqs_endpoint: str | None = None,
    redis_url: str | None = None,
    redis_queue: str | None = None,
    redis_prefix: str | None = None,
    log_socket: str | None = None,
    agent_socket: str | None = None,
) -> QueueConfig:
    """Code settings win over environment settings; managed worker assignment is authoritative.

    Only an explicit backend or managed configuration selects a mode. Ambient AWS
    credentials and REDIS_URL never select one. Managed credential providers are
    explicit (D13.4); no missing/unknown value falls back to the default AWS chain.
    """
    env = os.environ if env is None else env

    def setting(value: str | None, name: str, default: str | None = None) -> str | None:
        return value if value is not None else env.get(name, default)

    document = managed_config
    if document is None:
        document = env.get("LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG")
    mode = setting(backend, "LARAVEL_CLOUD_QUEUES_BACKEND")
    if mode is None and document is not None:
        mode = "managed"
    if mode not in ("managed", "sqs", "redis"):
        raise ConfigurationError("Select a backend: managed, sqs, or redis.")
    endpoint = setting(sqs_endpoint, "LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT")
    if endpoint is not None and document is not None:
        raise ConfigurationError("SQS endpoint overrides are forbidden with managed configuration.")
    log = _string(setting(log_socket, "LARAVEL_CLOUD_LOG_SOCKET", DEFAULT_LOG_SOCKET), "log_socket")

    if mode == "redis":
        return QueueConfig(
            mode="redis",
            log_socket=log,
            redis=RedisConfig(
                url=_string(
                    setting(redis_url, "LARAVEL_CLOUD_QUEUES_REDIS_URL", env.get("REDIS_URL")),
                    "redis_url",
                ),
                queue=_string(
                    setting(redis_queue, "LARAVEL_CLOUD_QUEUES_REDIS_QUEUE", DEFAULT_QUEUE),
                    "redis_queue",
                ),
                prefix=_string(
                    setting(
                        redis_prefix, "LARAVEL_CLOUD_QUEUES_REDIS_PREFIX", DEFAULT_REDIS_PREFIX
                    ),
                    "redis_prefix",
                    empty=True,
                ),
            ),
        )

    if mode == "managed":
        if isinstance(document, str):
            try:
                document = json.loads(document)
            except (ValueError, RecursionError):
                raise ConfigurationError("Managed configuration is not valid JSON.") from None
        raw = _object(document, "Managed configuration")
        if raw.get("driver") != "cloud":
            raise ConfigurationError("Managed configuration driver must be cloud.")
        conn = _object(raw.get("connection"), "connection")
        region = _string(conn.get("region"), "connection.region")
        credentials = conn.get("credentials")
        if credentials not in ("ecs", "instance"):
            raise ConfigurationError("Managed credentials must explicitly select ecs or instance.")
        if sqs_credentials is not None:
            if sqs_credentials not in ("ecs", "instance"):
                raise ConfigurationError(
                    "Managed credentials must explicitly select ecs or instance."
                )
            credentials = sqs_credentials
        connection = SqsConnectionConfig(
            prefix=_string(
                sqs_prefix if sqs_prefix is not None else conn.get("prefix", ""),
                "connection.prefix",
                empty=True,
            ),
            suffix=_string(
                sqs_suffix if sqs_suffix is not None else conn.get("suffix", ""),
                "connection.suffix",
                empty=True,
            ),
            queue=_string(
                sqs_queue if sqs_queue is not None else conn.get("queue", DEFAULT_QUEUE),
                "connection.queue",
            ),
            region=_string(sqs_region if sqs_region is not None else region, "connection.region"),
            credentials=credentials,
        )
        agent = _object(raw.get("agent", {}), "agent")
        socket = agent.get("socket")
        if socket is None or socket == "":
            socket = setting(agent_socket, "LARAVEL_CLOUD_AGENT_SOCKET", DEFAULT_AGENT_SOCKET)
        inventory = raw.get("queues", [])
        if not isinstance(inventory, (list, Mapping)):
            raise ConfigurationError("queues must be a list or object.")
        queues = tuple(_string(queue, "queues entry") for queue in inventory)
        overflow = _object(conn.get("overflow", {}), "connection.overflow")
        cache = _object(conn.get("credential_cache", {}), "connection.credential_cache")
        after_commit = _boolean(conn.get("after_commit", False), "connection.after_commit")
        managed = ManagedQueuesConfig(
            connection=connection,
            agent=AgentConfig(
                enabled=_boolean(agent.get("enabled", False), "agent.enabled"),
                socket=_string(socket, "agent.socket"),
            ),
            queue=_string(raw.get("queue", DEFAULT_QUEUE), "queue"),
            queues=queues,
            after_commit=after_commit,
            overflow=overflow,
            credential_cache=cache,
            raw=raw,
        )
        for name, options, consequence in (
            ("overflow", overflow, "oversized payloads are rejected"),
            ("credential_cache", cache, "each worker resolves its own credentials"),
        ):
            if _boolean(options.get("enabled", False), f"connection.{name}.enabled"):
                logging.getLogger("laravel_cloud_queues").warning(
                    "%s is not supported; %s.", name, consequence
                )
        return QueueConfig(mode="managed", sqs=connection, managed=managed, log_socket=log)

    direct_credentials = sqs_credentials
    if direct_credentials is None:
        credential_mode = env.get("LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS")
        if credential_mode is not None:
            if credential_mode != "default":
                raise ConfigurationError("SQS_CREDENTIALS must be default when set.")
            direct_credentials = "default"
        else:
            direct_credentials = StaticCredentials(
                key=_string(env.get("LARAVEL_CLOUD_QUEUES_SQS_KEY"), "SQS_KEY"),
                secret=_string(env.get("LARAVEL_CLOUD_QUEUES_SQS_SECRET"), "SQS_SECRET"),
            )
    if isinstance(direct_credentials, StaticCredentials):
        _string(direct_credentials.key, "SQS key")
        _string(direct_credentials.secret, "SQS secret")
    elif direct_credentials not in ("ecs", "instance", "default"):
        raise ConfigurationError("Unknown SQS direct_credentials provider.")
    return QueueConfig(
        mode="sqs",
        log_socket=log,
        sqs=SqsConnectionConfig(
            prefix=_string(setting(sqs_prefix, "LARAVEL_CLOUD_QUEUES_SQS_PREFIX"), "sqs_prefix"),
            region=_string(setting(sqs_region, "LARAVEL_CLOUD_QUEUES_SQS_REGION"), "sqs_region"),
            credentials=direct_credentials,
            suffix=_string(
                setting(sqs_suffix, "LARAVEL_CLOUD_QUEUES_SQS_SUFFIX", ""), "sqs_suffix", empty=True
            ),
            queue=_string(
                setting(sqs_queue, "LARAVEL_CLOUD_QUEUES_SQS_QUEUE", DEFAULT_QUEUE), "sqs_queue"
            ),
            endpoint_url=_string(endpoint, "sqs_endpoint") if endpoint is not None else None,
        ),
    )
