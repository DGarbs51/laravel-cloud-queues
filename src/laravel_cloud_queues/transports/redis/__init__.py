"""Redis/Valkey transport (``redis`` mode; needs the ``[redis]`` extra). CONTRACT STUB — lane L5.

Importing this module must work without ``redis`` installed: import ``redis`` inside the
class constructors and raise ConfigurationError with an install hint
(``pip install "laravel-cloud-queues[redis]"``) when it is missing.
"""

from __future__ import annotations

from collections.abc import Sequence

from ...config import RedisConfig
from ..base import Delivery, OutgoingMessage, SentMessage


class RedisProducer:
    def __init__(self, config: RedisConfig) -> None:
        raise NotImplementedError

    @property
    def max_payload_bytes(self) -> int | None:
        raise NotImplementedError

    @property
    def supports_fifo(self) -> bool:
        raise NotImplementedError

    def send(self, message: OutgoingMessage) -> SentMessage:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class RedisConsumer:
    """Laravel ``RedisQueue`` semantics with atomic Lua: pending list, ``:delayed`` and
    ``:reserved`` sorted sets (+ ``:notify`` list for bounded blocking pop)."""

    def __init__(self, config: RedisConfig, *, lease_seconds: int = 60) -> None:
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
