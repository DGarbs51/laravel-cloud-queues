"""Environment + code configuration loading. CONTRACT STUB — implemented by lane L3a."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._models import Mode, QueueConfig, SqsCredentials


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
    """Resolve configuration. Every keyword wins over its environment variable.

    ``env`` defaults to ``os.environ``. Selection (D6a): ``backend`` /
    ``LARAVEL_CLOUD_QUEUES_BACKEND`` = ``managed|sqs|redis``; when unset, ``managed``
    if ``LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG`` (or ``managed_config``) is present,
    otherwise :class:`~laravel_cloud_queues.errors.ConfigurationError`. ``REDIS_URL``
    and ``AWS_*`` never select a backend.

    ``sqs_credentials``: when omitted in ``sqs`` mode, built from ``_SQS_KEY``/``_SQS_SECRET``,
    or ``"default"`` when ``LARAVEL_CLOUD_QUEUES_SQS_CREDENTIALS=default``.

    ``agent_socket`` only applies when the managed config does not set ``agent.socket``
    (config, else ``LARAVEL_CLOUD_AGENT_SOCKET``, else default).

    Raises ConfigurationError for: malformed managed JSON, ``driver`` != ``cloud``,
    missing ``connection``/``region``, unknown ``credentials`` string, ``sqs`` mode without
    prefix/region/credentials, ``_SQS_ENDPOINT`` together with managed config, ``redis``
    mode without a URL, unknown backend value. Logs (never raises) startup warnings for
    ``overflow.enabled`` / ``credential_cache.enabled``. Never logs secrets.
    """
    raise NotImplementedError
