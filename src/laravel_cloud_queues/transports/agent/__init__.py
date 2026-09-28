"""Laravel Cloud in-container agent consumer (managed mode). CONTRACT STUB — lane L4."""

from __future__ import annotations

from collections.abc import Sequence

from ...config import ManagedQueuesConfig
from ..base import Delivery


class AgentConsumer:
    """HTTP over the agent Unix socket (httpx, base URL ``http://localhost``, no redirects,
    bounded response size). ``GET /next``: 65 s timeout, 3 attempts (retry after 0 ms,
    500 ms) on connection errors and HTTP error statuses (D13.3); 204 empty; 200 JSON
    object/array else fatal; missing/empty/non-string ``messageId`` = empty poll;
    non-string ``receiptHandle`` -> None; non-string ``body`` -> "". Other statuses /
    unreachable -> AgentUnavailableError.
    ``POST /result``: body ``messageId``, ``receiptHandle`` (omitted if None), ``status``
    (``processed``|``released``), ``delay`` (omitted if None; 0 kept); 10 s timeout, 3
    attempts 100 ms apart on connection errors only; 5xx / connection failure ->
    AgentUnavailableError; 4xx -> AgentProtocolError."""

    def __init__(self, managed: ManagedQueuesConfig) -> None:
        raise NotImplementedError

    @property
    def supports_renewal(self) -> bool:
        raise NotImplementedError

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        raise NotImplementedError

    def complete(self, delivery: Delivery) -> None:
        raise NotImplementedError

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        raise NotImplementedError

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        raise NotImplementedError

    def interrupt(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
