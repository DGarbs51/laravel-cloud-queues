from contextlib import contextmanager
from dataclasses import replace
from time import monotonic, sleep

import pytest

from laravel_cloud_queues.config import SqsConnectionConfig, StaticCredentials, load_config
from laravel_cloud_queues.errors import (
    LeaseLostError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
)
from laravel_cloud_queues.transports.base import OutgoingMessage
from laravel_cloud_queues.transports.sqs import SqsConsumer, SqsProducer

pytestmark = pytest.mark.sqs


@contextmanager
def transports(endpoint, url, *, lease=60):
    prefix, queue = url.rsplit("/", 1)
    connection = SqsConnectionConfig(
        prefix,
        endpoint.region,
        StaticCredentials(endpoint.access_key, endpoint.secret_key),
        queue=queue,
        endpoint_url=endpoint.url,
    )
    producer, consumer = SqsProducer(connection), SqsConsumer(connection, lease_seconds=lease)
    try:
        yield queue, producer, consumer
    finally:
        consumer.close()
        producer.close()


def test_send_receive_visibility_retry_and_delete(sqs_endpoint):
    """Queue/Jobs/SqsJob.php:81,107,132: same message, fresh receipt, attempt increment."""
    url = sqs_endpoint.create_queue()
    with transports(sqs_endpoint, url) as (queue, producer, consumer):
        sent = producer.send(OutgoingMessage("héllo", queue))
        first = consumer.receive([queue], 0)
        assert (first.message_id, first.queue, first.body, first.attempt) == (
            sent.message_id,
            queue,
            "héllo",
            1,
        )
        assert consumer.receive([queue], 0) is None
        consumer.release(first, 0)
        second = consumer.receive([queue], 0)
        assert second.message_id == first.message_id
        assert second.receipt != first.receipt
        assert second.attempt == 2
        consumer.complete(second)
        assert consumer.receive([queue], 0) is None
        with pytest.raises(LeaseLostError):
            consumer.renew(second, 60)


def test_named_queue_suffix_and_standard_delay(sqs_endpoint):
    """Queue/SqsQueue.php:589,703: delay and named queue suffix."""
    url = sqs_endpoint.create_queue()
    with transports(sqs_endpoint, url) as (queue, producer, consumer):
        suffix = queue[-4:]
        producer._connection = replace(producer._connection, suffix=suffix)
        consumer._connection = producer._connection
        logical = queue[:-4]
        producer.send(OutgoingMessage("delayed", logical, delay_seconds=1))
        assert consumer.receive([logical], 0) is None
        sleep(1.1)
        delivery = consumer.receive([logical], 2)
        assert delivery.queue == logical
        consumer.complete(delivery)


def test_fifo_attributes_and_deduplication(sqs_endpoint):
    """Queue/SqsQueue.php:599-633: FIFO group and deduplication ID."""
    url = sqs_endpoint.create_queue(fifo=True)
    with transports(sqs_endpoint, url) as (queue, producer, consumer):
        first = producer.send(
            OutgoingMessage("body", queue, fifo_group="orders", deduplication_id="business-key")
        )
        producer.send(
            OutgoingMessage("body", queue, fifo_group="orders", deduplication_id="business-key")
        )
        response = sqs_endpoint.client.receive_message(
            QueueUrl=url, MessageSystemAttributeNames=["All"]
        )
        message = response["Messages"][0]
        assert message["MessageId"] == first.message_id
        assert message["Attributes"]["MessageGroupId"] == "orders"
        assert message["Attributes"]["MessageDeduplicationId"] == "business-key"
        sqs_endpoint.client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        assert consumer.receive([queue], 0) is None
        # None omits the dedup ID and activates queue content-based deduplication.
        producer.send(OutgoingMessage("content-dedup", queue, fifo_group="orders"))
        producer.send(OutgoingMessage("content-dedup", queue, fifo_group="orders"))
        delivery = consumer.receive([queue], 0)
        assert delivery.body == "content-dedup"
        consumer.complete(delivery)
        assert consumer.receive([queue], 0) is None


def test_fair_queue_group_attribute(sqs_endpoint):
    """Queue/SqsQueue.php:606-614: standard-queue MessageGroupId (emulated, not fairness)."""
    url = sqs_endpoint.create_queue()
    with transports(sqs_endpoint, url) as (queue, producer, _):
        producer.send(OutgoingMessage("body", queue, message_group="tenant"))
        response = sqs_endpoint.client.receive_message(
            QueueUrl=url, MessageSystemAttributeNames=["All"]
        )
        assert response["Messages"][0]["Attributes"]["MessageGroupId"] == "tenant"


def test_queue_not_found_and_lower_size_limit(sqs_endpoint):
    url = sqs_endpoint.create_queue()
    sqs_endpoint.client.set_queue_attributes(
        QueueUrl=url, Attributes={"MaximumMessageSize": "1024"}
    )
    with transports(sqs_endpoint, url) as (queue, producer, _):
        with pytest.raises(ManagedQueueNotFoundError) as missing:
            producer.send(OutgoingMessage("body", queue + "-absent"))
        assert missing.value.queue == queue + "-absent"
        with pytest.raises(PayloadTooLargeError) as oversized:
            producer.send(OutgoingMessage("x" * 1025, queue))
        assert (oversized.value.size, oversized.value.limit) == (1025, 1024)


def test_explicit_config_works_with_hostile_aws_environment(sqs_endpoint, monkeypatch):
    url = sqs_endpoint.create_queue()
    prefix, queue = url.rsplit("/", 1)
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_SQS",
    ):
        monkeypatch.setenv(key, "garbage")
    config = load_config(
        env={},
        backend="sqs",
        sqs_prefix=prefix,
        sqs_queue=queue,
        sqs_region=sqs_endpoint.region,
        sqs_credentials=StaticCredentials(sqs_endpoint.access_key, sqs_endpoint.secret_key),
        sqs_endpoint=sqs_endpoint.url,
    )
    producer, consumer = SqsProducer(config.sqs), SqsConsumer(config.sqs)
    try:
        sent = producer.send(OutgoingMessage("explicit", queue))
        delivery = consumer.receive([queue], 0)
        assert delivery.message_id == sent.message_id
        consumer.complete(delivery)
    finally:
        consumer.close()
        producer.close()


def test_renewal_keeps_message_invisible_past_original_lease(sqs_endpoint):
    url = sqs_endpoint.create_queue()
    with transports(sqs_endpoint, url, lease=2) as (queue, producer, consumer):
        producer.send(OutgoingMessage("long-job", queue))
        delivery = consumer.receive([queue], 0)
        started = monotonic()
        sleep(1)
        consumer.renew(delivery, 4)
        sleep(1.2)
        assert monotonic() - started > 2
        assert consumer.receive([queue], 0) is None
        consumer.complete(delivery)
