from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import get_ident
from unittest.mock import Mock

import boto3.session
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.stub import Stubber

from laravel_cloud_queues.config import SqsConnectionConfig, StaticCredentials
from laravel_cloud_queues.errors import (
    AmbiguousAcknowledgementError,
    ConfigurationError,
    LeaseLostError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
    TransportError,
)
from laravel_cloud_queues.transports.base import Delivery, OutgoingMessage
from laravel_cloud_queues.transports.sqs import SqsConsumer, SqsProducer
from laravel_cloud_queues.transports.sqs import _client

CONNECTION = SqsConnectionConfig(
    "https://sqs.us-east-1.amazonaws.com/123",
    "us-east-1",
    StaticCredentials("test", "secret"),
    suffix="-s",
)
URL = CONNECTION.prefix + "/emails-s"
DELIVERY = Delivery(
    "message", "emails", "body", 1, receipt="sensitive-receipt", meta={"queue_url": URL}
)


@pytest.fixture
def client():
    sdk = boto3.session.Session(
        aws_access_key_id="test", aws_secret_access_key="secret", region_name="us-east-1"
    ).client("sqs", config=Config(ignore_configured_endpoint_urls=True))
    yield sdk
    sdk.close()


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"delay_seconds": 5},
        {"fifo_group": "group", "deduplication_id": "dedup"},
        {"message_group": "tenant"},
    ],
)
def test_send_arguments(client, options):
    """Queue/SqsQueue.php:320,579: body and FIFO/fair attributes passed to SQS."""
    producer = SqsProducer(CONNECTION)
    producer._client = client
    expected = {"QueueUrl": URL, "MessageBody": "body"}
    for source, target in [
        ("delay_seconds", "DelaySeconds"),
        ("fifo_group", "MessageGroupId"),
        ("message_group", "MessageGroupId"),
        ("deduplication_id", "MessageDeduplicationId"),
    ]:
        if source in options:
            expected[target] = options[source]
    with Stubber(client) as stub:
        stub.add_response("send_message", {"MessageId": "message"}, expected)
        assert producer.send(OutgoingMessage("body", "emails", **options)).message_id == "message"
        stub.assert_no_pending_responses()
    assert producer.max_payload_bytes == 1048576
    assert producer.supports_fifo


@pytest.mark.parametrize(
    ("code", "reason", "error"),
    [
        ("AWS.SimpleQueueService.NonExistentQueue", "secret", ManagedQueueNotFoundError),
        ("QueueDoesNotExist", "secret", ManagedQueueNotFoundError),
        (
            "InvalidParameterValue",
            "Message must be shorter than 1024 bytes. sensitive-receipt",
            PayloadTooLargeError,
        ),
        ("InvalidParameterValue", "Invalid MessageGroupId: sensitive-receipt", TransportError),
        ("AccessDenied", "secret", TransportError),
    ],
)
def test_send_error_mapping(client, code, reason, error):
    """Foundation/Cloud/QueueConnector.php:79 not-found translation; §8 size errors."""
    producer = SqsProducer(CONNECTION)
    producer._client = client
    with Stubber(client) as stub:
        stub.add_client_error("send_message", service_error_code=code, service_message=reason)
        with pytest.raises(error) as raised:
            producer.send(OutgoingMessage("é" * 600, URL))
    assert "secret" not in str(raised.value)
    assert "sensitive-receipt" not in str(raised.value)
    assert raised.value.__suppress_context__
    if error is ManagedQueueNotFoundError:
        assert raised.value.queue == "emails"
    if error is PayloadTooLargeError:
        assert (raised.value.size, raised.value.limit, raised.value.queue) == (1200, 1024, "emails")


@pytest.mark.parametrize(
    ("queues", "wait", "expected_wait"),
    [(["emails"], 99, 20), (["emails"], 3.9, 3), (["first", "emails"], 20, 0)],
)
def test_receive_priority_and_parameters(client, queues, wait, expected_wait):
    """Queue/SqsQueue.php:644; D13.5/10 long-poll and missing-count deviations."""
    consumer = SqsConsumer(CONNECTION, lease_seconds=17)
    consumer._client = client
    with Stubber(client) as stub:
        for queue in queues:
            response = (
                {"Messages": [{"MessageId": "message", "Body": "body", "ReceiptHandle": "receipt"}]}
                if queue == "emails"
                else {}
            )
            stub.add_response(
                "receive_message",
                response,
                {
                    "QueueUrl": CONNECTION.prefix + f"/{queue}-s",
                    "WaitTimeSeconds": expected_wait,
                    "MaxNumberOfMessages": 1,
                    "MessageSystemAttributeNames": ["ApproximateReceiveCount"],
                    "VisibilityTimeout": 17,
                },
            )
        delivery = consumer.receive(queues, wait)
        assert (
            delivery.message_id,
            delivery.body,
            delivery.queue,
            delivery.attempt,
            delivery.receipt,
        ) == ("message", "body", "emails", 1, "receipt")
        assert delivery.meta == {"queue_url": URL}
        assert delivery.received_at > 0
        stub.assert_no_pending_responses()
    assert consumer.supports_renewal


def test_priority_stops_at_first_message_and_interrupt_preserves_handoff():
    consumer = SqsConsumer(CONNECTION)
    client = Mock()
    consumer._client = client

    def receive(**kwargs):
        consumer.interrupt()
        return {
            "Messages": [
                {
                    "MessageId": "id",
                    "Body": "body",
                    "ReceiptHandle": "receipt",
                    "Attributes": {"ApproximateReceiveCount": "3"},
                }
            ]
        }

    client.receive_message.side_effect = receive
    assert consumer.receive(["emails", "other"], 20).attempt == 3
    assert consumer.receive(["emails"], 20) is None
    client.receive_message.assert_called_once()


def test_empty_receive_and_interrupt_do_not_create_client():
    consumer = SqsConsumer(CONNECTION)
    assert consumer.receive([], 0) is None
    consumer.interrupt()
    assert consumer.receive(["emails"], 0) is None
    assert consumer._client is None


def test_settlement_uses_delivery_url_and_latest_receipt(client):
    """Queue/Jobs/SqsJob.php:81,107: visibility retry/delete uses the delivery receipt."""
    consumer = SqsConsumer(CONNECTION)
    consumer._client = client
    latest = replace(DELIVERY, receipt="latest")
    with Stubber(client) as stub:
        stub.add_response(
            "change_message_visibility",
            {},
            {"QueueUrl": URL, "ReceiptHandle": latest.receipt, "VisibilityTimeout": 43200},
        )
        stub.add_response(
            "change_message_visibility",
            {},
            {"QueueUrl": URL, "ReceiptHandle": latest.receipt, "VisibilityTimeout": 60},
        )
        stub.add_response("delete_message", {}, {"QueueUrl": URL, "ReceiptHandle": latest.receipt})
        consumer.release(latest, 50000)
        consumer.renew(latest, 60)
        consumer.complete(latest)
        stub.assert_no_pending_responses()


@pytest.mark.parametrize("method", ["complete", "release", "renew"])
@pytest.mark.parametrize(
    ("code", "reason"),
    [
        ("ReceiptHandleIsInvalid", "sensitive-receipt"),
        ("MessageNotInflight", "sensitive-receipt"),
        ("InvalidParameterValue", "Receipt handle sensitive-receipt has expired."),
    ],
)
def test_lost_receipt_is_fatal(client, method, code, reason):
    consumer = SqsConsumer(CONNECTION)
    consumer._client = client
    with Stubber(client) as stub:
        stub.add_client_error(
            "delete_message" if method == "complete" else "change_message_visibility",
            service_error_code=code,
            service_message=reason,
        )
        with pytest.raises(LeaseLostError) as raised:
            getattr(consumer, method)(DELIVERY, *([] if method == "complete" else [60]))
    assert "sensitive-receipt" not in str(raised.value)
    assert raised.value.__suppress_context__


@pytest.mark.parametrize(
    ("method", "error"),
    [
        ("send", TransportError),
        ("receive", TransportError),
        ("complete", AmbiguousAcknowledgementError),
        ("release", AmbiguousAcknowledgementError),
        ("renew", LeaseLostError),
    ],
)
def test_network_failure_contract(method, error):
    transport = SqsProducer(CONNECTION) if method == "send" else SqsConsumer(CONNECTION)
    transport._client = Mock()
    operation = {
        "send": "send_message",
        "receive": "receive_message",
        "complete": "delete_message",
        "release": "change_message_visibility",
        "renew": "change_message_visibility",
    }[method]
    getattr(transport._client, operation).side_effect = EndpointConnectionError(
        endpoint_url="https://secret:sensitive-receipt@example"
    )
    args = {
        "send": [OutgoingMessage("body", "emails")],
        "receive": [["emails"], 0],
        "complete": [DELIVERY],
        "release": [DELIVERY, 2],
        "renew": [DELIVERY, 60],
    }[method]
    with pytest.raises(error) as raised:
        getattr(transport, method)(*args)
    assert "sensitive-receipt" not in str(raised.value)
    assert raised.value.__suppress_context__


def test_non_receipt_ack_rejection_stops_worker():
    consumer = SqsConsumer(CONNECTION)
    consumer._client = Mock()
    consumer._client.change_message_visibility.side_effect = ClientError(
        {
            "Error": {
                "Code": "InvalidParameterValue",
                "Message": "VisibilityTimeout exceeds maximum",
            }
        },
        "ChangeMessageVisibility",
    )
    with pytest.raises(AmbiguousAcknowledgementError):
        consumer.release(DELIVERY, 43200)


def test_client_is_lazy_and_created_once_in_calling_thread(monkeypatch):
    creator_threads = []
    client = Mock()
    client.send_message.return_value = {"MessageId": "message"}

    def build(connection):
        creator_threads.append(get_ident())
        return client

    monkeypatch.setattr(_client, "_build_client", build)
    producer = SqsProducer(CONNECTION)
    assert not creator_threads
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(lambda _: producer.send(OutgoingMessage("body", "emails")), range(20))
        )
    assert len(results) == 20
    assert len(creator_threads) == 1
    assert creator_threads[0] != get_ident()
    producer.close()
    producer.close()
    client.close.assert_called_once()
    with pytest.raises(TransportError, match="closed"):
        producer.send(OutgoingMessage("body", "emails"))


@pytest.mark.parametrize("lease", [0, -1, 43201])
def test_invalid_lease(lease):
    with pytest.raises(ConfigurationError):
        SqsConsumer(CONNECTION, lease_seconds=lease)
