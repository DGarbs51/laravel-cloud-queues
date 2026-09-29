"""The frozen configuration models, whose ``repr`` never reveals a secret."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

Mode = Literal["managed", "sqs", "redis"]
"""The name of a queue backend."""

DEFAULT_AGENT_SOCKET = "/tmp/cloud-agent.sock"
"""The default path of the Laravel Cloud agent socket."""
DEFAULT_LOG_SOCKET = "unix:///tmp/cloud-init.sock"
"""The default address of the Laravel Cloud log socket."""
DEFAULT_REDIS_PREFIX = "laravel-cloud-queues:"
"""The default prefix for Redis keys."""
DEFAULT_QUEUE = "default"
"""The name of the default queue."""


@dataclass(frozen=True)
class StaticCredentials:
    """Explicit SQS credentials for ``sqs`` mode, which are never logged."""

    key: str
    """The AWS access key ID."""
    secret: str = field(repr=False)
    """The AWS secret access key."""
    token: str | None = field(default=None, repr=False)
    """The optional AWS session token."""


SqsCredentials = Literal["ecs", "instance", "default"] | StaticCredentials
"""The source of the SQS credentials.

``ecs`` and ``instance`` select explicit, refreshable AWS providers (managed mode).
``default`` uses the boto3 default chain, only on explicit opt-in (``sqs`` mode).
A :class:`StaticCredentials` instance supplies an explicit key and secret (``sqs`` mode).
"""


@dataclass(frozen=True)
class SqsConnectionConfig:
    """The settings needed to build the boto3 SQS client and queue URLs."""

    prefix: str
    """The queue URL prefix, e.g. ``https://sqs.us-east-2.amazonaws.com/123456789012``."""
    region: str
    """The AWS region of the queues."""
    credentials: SqsCredentials = field(repr=False)
    """The source of the SQS credentials."""
    suffix: str = ""
    """The suffix appended to every queue name."""
    queue: str = DEFAULT_QUEUE
    """The default queue for dispatch."""
    endpoint_url: str | None = None
    """The custom SQS endpoint.

    It comes only from ``LARAVEL_CLOUD_QUEUES_SQS_ENDPOINT`` or code, never ``AWS_ENDPOINT_URL*``.
    """


@dataclass(frozen=True)
class AgentConfig:
    """The settings for the Laravel Cloud agent."""

    enabled: bool
    """Indicates if the agent is enabled."""
    socket: str = DEFAULT_AGENT_SOCKET
    """The path of the agent socket."""


@dataclass(frozen=True)
class ManagedQueuesConfig:
    """The parsed ``LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`` document.

    Laravel Cloud's worker assignment wins over any other setting.
    """

    connection: SqsConnectionConfig
    """The SQS connection settings."""
    agent: AgentConfig
    """The Laravel Cloud agent settings."""
    queue: str = DEFAULT_QUEUE
    """The assigned worker queue: the top-level ``queue``, else ``default``."""
    queues: tuple[str, ...] = ()
    """The queue inventory (a list, or object keys), which is never a dispatch allowlist."""
    after_commit: bool = False
    """Indicates if jobs should be dispatched after database commits."""
    overflow: Mapping[str, object] = field(default_factory=dict[str, object])
    """The oversized payload settings, which are not supported."""
    credential_cache: Mapping[str, object] = field(default_factory=dict[str, object])
    """The shared credential cache settings, which are not supported."""
    raw: Mapping[str, object] = field(default_factory=dict[str, object], repr=False)
    """The full decoded document, with unknown fields kept for forward compatibility."""


@dataclass(frozen=True)
class RedisConfig:
    """The settings for the Redis backend."""

    url: str = field(repr=False)
    """The ``redis://`` or ``rediss://`` URL (TLS, with verification on)."""
    queue: str = DEFAULT_QUEUE
    """The default queue for dispatch."""
    prefix: str = DEFAULT_REDIS_PREFIX
    """The prefix for Redis keys."""


@dataclass(frozen=True)
class QueueConfig:
    """The resolved configuration for one process, with exactly one backend section set."""

    mode: Mode
    """The selected queue backend."""
    sqs: SqsConnectionConfig | None = None
    """The SQS settings, set in ``sqs`` mode and in ``managed`` mode (``managed.connection``)."""
    redis: RedisConfig | None = None
    """The Redis settings, set in ``redis`` mode."""
    managed: ManagedQueuesConfig | None = None
    """The managed settings, set in ``managed`` mode."""
    log_socket: str = DEFAULT_LOG_SOCKET
    """The log socket from ``LARAVEL_CLOUD_LOG_SOCKET``, used in managed mode only."""

    @property
    def default_queue(self) -> str:
        """Get the queue used when neither the job nor ``.options()`` names one."""
        if self.redis is not None:
            return self.redis.queue
        if self.sqs is not None:
            return self.sqs.queue
        return DEFAULT_QUEUE

    @property
    def emits_cloud_events(self) -> bool:
        """Determine if lifecycle and ``failed_job`` events go to the log socket (managed only)."""
        return self.mode == "managed"

    @property
    def uses_agent(self) -> bool:
        """Determine if the Laravel Cloud agent is enabled."""
        return self.managed is not None and self.managed.agent.enabled
