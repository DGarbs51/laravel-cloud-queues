"""Configuration and backend selection (PROJECT_SCOPE.md §6, D6a)."""

from __future__ import annotations

from ._loader import load_config
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

__all__ = [
    "DEFAULT_AGENT_SOCKET",
    "DEFAULT_LOG_SOCKET",
    "DEFAULT_QUEUE",
    "DEFAULT_REDIS_PREFIX",
    "AgentConfig",
    "ManagedQueuesConfig",
    "Mode",
    "QueueConfig",
    "RedisConfig",
    "SqsConnectionConfig",
    "SqsCredentials",
    "StaticCredentials",
    "load_config",
]
