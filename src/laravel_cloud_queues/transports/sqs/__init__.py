"""Body-opaque direct SQS transport for managed queues and explicit SQS backends."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from threading import Event
from time import monotonic
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError

from ...config import SqsConnectionConfig
from ...errors import (
    AmbiguousAcknowledgementError,
    ConfigurationError,
    LeaseLostError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
    TransportError,
)
from ..base import (
    MAX_VISIBILITY_SECONDS,
    SQS_MAX_PAYLOAD_BYTES,
    Delivery,
    OutgoingMessage,
    SentMessage,
)
from ._client import SqsTransport

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import SendMessageRequestTypeDef


def queue_url(connection: SqsConnectionConfig, queue: str) -> str:
    """Laravel ``SqsQueue::getQueue``/``suffixQueue``: full URLs pass through; prefix trailing
    ``/`` trimmed; suffix appended once (``Str::finish``); FIFO ``{base}{suffix}.fifo``."""
    queue = queue or connection.queue
    parsed = urlsplit(queue)
    if parsed.scheme and parsed.netloc:
        return queue
    fifo = ".fifo" if queue.endswith(".fifo") else ""
    base = queue.removesuffix(fifo) if fifo else queue
    if connection.suffix:
        while base.endswith(connection.suffix):
            base = base.removesuffix(connection.suffix)
    return f"{connection.prefix.rstrip('/')}/{base}{connection.suffix}{fifo}"


def normalize_queue(connection: SqsConnectionConfig, queue_or_url: str) -> str:
    """Inverse of :func:`queue_url` (Laravel Cloud ``Queue::normalizeQueue``): logical name."""
    name = queue_or_url.removeprefix(connection.prefix.rstrip("/") + "/")
    fifo = ".fifo" if name.endswith(".fifo") else ""
    base = name.removesuffix(fifo) if fifo else name
    return base.removesuffix(connection.suffix) + fifo


def receive_count(value: object) -> int:
    """``ApproximateReceiveCount`` as an attempt number; missing or non-numeric is 1.

    Labeled deviation ``missing-receive-count-is-one`` (D13.5, Symfony parity), shared
    with the agent path so both consumers count attempts identically.
    """
    try:
        return max(1, int(value)) if isinstance(value, (str, int, float)) else 1
    except (ValueError, OverflowError):
        return 1


class SqsProducer(SqsTransport):
    """Send using explicit settings; client/credentials are resolved on first use."""

    @property
    def max_payload_bytes(self) -> int | None:
        return SQS_MAX_PAYLOAD_BYTES

    @property
    def supports_fifo(self) -> bool:
        return True

    def send(self, message: OutgoingMessage) -> SentMessage:
        url = queue_url(self._connection, message.queue)
        params: SendMessageRequestTypeDef = {"QueueUrl": url, "MessageBody": message.body}
        if message.delay_seconds:
            params["DelaySeconds"] = message.delay_seconds
        group = message.fifo_group if message.fifo_group is not None else message.message_group
        if group is not None:
            params["MessageGroupId"] = group
        if message.deduplication_id is not None:
            params["MessageDeduplicationId"] = message.deduplication_id
        try:
            response = self._get_client().send_message(**params)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            logical = normalize_queue(self._connection, url)
            if code in ("AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"):
                raise ManagedQueueNotFoundError(logical) from None
            reason = exc.response.get("Error", {}).get("Message", "")
            size_limit = re.search(r"[Mm]essage must be shorter than (\d+) bytes", reason)
            if code == "InvalidParameterValue" and size_limit:
                raise PayloadTooLargeError(
                    size=len(message.body.encode("utf-8")),
                    limit=int(size_limit.group(1)),
                    queue=logical,
                ) from None
            raise TransportError("SQS send failed.") from None
        except BotoCoreError:
            raise TransportError("SQS send failed.") from None
        return SentMessage(message_id=response["MessageId"], queue=message.queue)


class SqsConsumer(SqsTransport):
    """One message per poll; retries change visibility on the original delivery.

    interrupt() prevents the next poll, but cannot cancel an in-flight SDK long
    poll. A message returned by that poll is still handed to the worker.
    """

    def __init__(self, connection: SqsConnectionConfig, *, lease_seconds: int = 60) -> None:
        if not 0 < lease_seconds <= MAX_VISIBILITY_SECONDS:
            raise ConfigurationError("SQS lease_seconds must be between 1 and 43200.")
        super().__init__(connection)
        self._lease_seconds = lease_seconds
        self._interrupted = Event()

    @property
    def supports_renewal(self) -> bool:
        return True

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        if not math.isfinite(wait_seconds) or wait_seconds < 0:
            raise ValueError("wait_seconds must be finite and nonnegative.")
        wait = min(20, int(wait_seconds)) if len(queues) == 1 else 0
        for queue in queues:
            if self._interrupted.is_set():
                return None
            url = queue_url(self._connection, queue)
            try:
                response = self._get_client().receive_message(
                    QueueUrl=url,
                    WaitTimeSeconds=wait,
                    MaxNumberOfMessages=1,
                    MessageSystemAttributeNames=["ApproximateReceiveCount"],
                    VisibilityTimeout=self._lease_seconds,
                )
            except (ClientError, BotoCoreError):
                raise TransportError("SQS receive failed.") from None
            messages = response.get("Messages", [])
            if messages:
                message = messages[0]
                try:
                    return Delivery(
                        message_id=message["MessageId"],
                        queue=normalize_queue(self._connection, url),
                        body=message["Body"],
                        attempt=receive_count(
                            message.get("Attributes", {}).get("ApproximateReceiveCount")
                        ),
                        receipt=message["ReceiptHandle"],
                        received_at=monotonic(),
                        meta={"queue_url": url},
                    )
                except KeyError:
                    raise TransportError("SQS returned an invalid delivery.") from None
        return None

    def complete(self, delivery: Delivery) -> None:
        self._report(delivery)

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        limit = MAX_VISIBILITY_SECONDS
        if delivery.received_at > 0:
            # SQS measures its 12-hour ceiling from the original receive, not this
            # update. Reserve a second for transit; server rejection remains fatal.
            elapsed = max(0.0, monotonic() - delivery.received_at)
            limit = max(0, limit - math.ceil(elapsed) - 1)
        self._report(delivery, visibility=max(0, min(limit, delay_seconds)))

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        if not 0 < lease_seconds <= MAX_VISIBILITY_SECONDS:
            raise ValueError("SQS lease_seconds must be between 1 and 43200.")
        self._report(delivery, visibility=lease_seconds, renewing=True)

    def _report(
        self, delivery: Delivery, *, visibility: int | None = None, renewing: bool = False
    ) -> None:
        if not delivery.receipt:
            raise LeaseLostError("SQS delivery has no receipt handle.")
        url = delivery.meta.get("queue_url") or queue_url(self._connection, delivery.queue)
        try:
            client = self._get_client()
            if visibility is None:
                client.delete_message(QueueUrl=url, ReceiptHandle=delivery.receipt)
            else:
                client.change_message_visibility(
                    QueueUrl=url, ReceiptHandle=delivery.receipt, VisibilityTimeout=visibility
                )
        except ClientError as exc:
            error = exc.response.get("Error", {})
            code = error.get("Code", "")
            reason = error.get("Message", "").lower()
            lost = code in ("ReceiptHandleIsInvalid", "MessageNotInflight") or (
                code == "InvalidParameterValue"
                and ("receipt" in reason or "not inflight" in reason or "not in flight" in reason)
            )
            if renewing or lost:
                raise LeaseLostError("SQS delivery lease was lost.") from None
            raise AmbiguousAcknowledgementError("SQS outcome could not be confirmed.") from None
        except (BotoCoreError, TransportError):
            if renewing:
                raise LeaseLostError("SQS delivery lease could not be renewed.") from None
            raise AmbiguousAcknowledgementError("SQS outcome could not be confirmed.") from None

    def interrupt(self) -> None:
        self._interrupted.set()

    def close(self) -> None:
        self.interrupt()
        super().close()
