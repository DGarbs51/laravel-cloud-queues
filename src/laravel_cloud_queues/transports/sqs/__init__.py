"""Direct SQS transport (managed without agent, and ``sqs`` mode). CONTRACT STUB — lane L3a."""

from __future__ import annotations

from collections.abc import Sequence

from ...config import SqsConnectionConfig
from ..base import Delivery, OutgoingMessage, SentMessage


def queue_url(connection: SqsConnectionConfig, queue: str) -> str:
    """Laravel ``SqsQueue::getQueue``/``suffixQueue``: full URLs pass through; prefix trailing
    ``/`` trimmed; suffix appended once (``Str::finish``); FIFO ``{base}{suffix}.fifo``."""
    raise NotImplementedError


def normalize_queue(connection: SqsConnectionConfig, queue_or_url: str) -> str:
    """Inverse of :func:`queue_url` (Laravel Cloud ``Queue::normalizeQueue``): logical name."""
    raise NotImplementedError


class SqsProducer:
    """boto3 SQS producer. Client built only from explicit config (never ``AWS_*`` env or
    boto3 endpoint env). Credentials resolved lazily, in the calling thread."""

    def __init__(self, connection: SqsConnectionConfig) -> None:
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


class SqsConsumer:
    """ReceiveMessage (WaitTimeSeconds up to 20, MaxNumberOfMessages=1, receive count
    requested, VisibilityTimeout=lease), DeleteMessage, ChangeMessageVisibility."""

    def __init__(self, connection: SqsConnectionConfig, *, lease_seconds: int = 60) -> None:
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
