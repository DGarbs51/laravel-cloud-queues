"""Configuration models (PROJECT_SCOPE.md §6, D6/D6a). Frozen, secret-safe ``repr``."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

Mode = Literal["managed", "sqs", "redis"]

DEFAULT_AGENT_SOCKET = "/tmp/cloud-agent.sock"
DEFAULT_LOG_SOCKET = "unix:///tmp/cloud-init.sock"
DEFAULT_REDIS_PREFIX = "laravel-cloud-queues:"
DEFAULT_QUEUE = "default"


@dataclass(frozen=True)
class StaticCredentials:
    """Explicit SQS credentials (``sqs`` mode). Never logged."""

    key: str
    secret: str = field(repr=False)
    token: str | None = field(default=None, repr=False)


SqsCredentials = Literal["ecs", "instance", "default"] | StaticCredentials
"""``ecs``/``instance``: explicit refreshable AWS providers (managed mode).
``default``: boto3 default chain, only on explicit opt-in (``sqs`` mode).
:class:`StaticCredentials`: explicit key/secret (``sqs`` mode)."""


@dataclass(frozen=True)
class SqsConnectionConfig:
    """Everything needed to build the boto3 SQS client and queue URLs."""

    prefix: str
    """Queue URL prefix, e.g. ``https://sqs.us-east-2.amazonaws.com/123456789012``."""
    region: str
    credentials: SqsCredentials = field(repr=False)
    suffix: str = ""
    queue: str = DEFAULT_QUEUE
    """Default queue for dispatch."""
    endpoint_url: str | None = None
    """Only from ``LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT`` or code; never ``AWS_ENDPOINT_URL*``."""


@dataclass(frozen=True)
class AgentConfig:
    enabled: bool
    socket: str = DEFAULT_AGENT_SOCKET


@dataclass(frozen=True)
class ManagedQueuesConfig:
    """Parsed ``LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG``; Laravel Cloud's assignment wins."""

    connection: SqsConnectionConfig
    agent: AgentConfig
    queue: str = DEFAULT_QUEUE
    """Worker assignment: top-level ``queue``, else ``default``."""
    queues: tuple[str, ...] = ()
    """Inventory only (list, or object keys); never a dispatch allowlist."""
    after_commit: bool = False
    overflow: Mapping[str, Any] = field(default_factory=dict)
    credential_cache: Mapping[str, Any] = field(default_factory=dict)
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)
    """The full decoded document; unknown fields preserved for forward compatibility."""


@dataclass(frozen=True)
class RedisConfig:
    url: str = field(repr=False)
    """``redis://`` or ``rediss://`` (TLS, verification on)."""
    queue: str = DEFAULT_QUEUE
    prefix: str = DEFAULT_REDIS_PREFIX


@dataclass(frozen=True)
class QueueConfig:
    """Resolved configuration for one process. Exactly one backend section is set."""

    mode: Mode
    sqs: SqsConnectionConfig | None = None
    """Set for ``sqs`` mode, and for ``managed`` mode (``managed.connection``)."""
    redis: RedisConfig | None = None
    managed: ManagedQueuesConfig | None = None
    log_socket: str = DEFAULT_LOG_SOCKET
    """``LARAVEL_CLOUD_LOG_SOCKET`` or default. Used in ``managed`` mode only (D12)."""

    @property
    def default_queue(self) -> str:
        """Queue used by dispatch when neither the job nor ``.options()`` names one."""
        if self.redis is not None:
            return self.redis.queue
        if self.sqs is not None:
            return self.sqs.queue
        return DEFAULT_QUEUE

    @property
    def emits_cloud_events(self) -> bool:
        """Lifecycle and ``failed_job`` events go to the log socket only in managed mode (D12)."""
        return self.mode == "managed"

    @property
    def uses_agent(self) -> bool:
        return self.managed is not None and self.managed.agent.enabled
