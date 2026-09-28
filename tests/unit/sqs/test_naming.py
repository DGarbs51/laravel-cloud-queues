import pytest

from laravel_cloud_queues.config import SqsConnectionConfig
from laravel_cloud_queues.transports.sqs import normalize_queue, queue_url


@pytest.mark.parametrize(
    ("prefix", "suffix", "name", "expected", "logical"),
    [
        ("https://sqs.example/123", "-s", "name", "https://sqs.example/123/name-s", "name"),
        ("https://sqs.example/123///", "-s", "name-s", "https://sqs.example/123/name-s", "name"),
        ("https://sqs.example/123", "-s", "name-s-s", "https://sqs.example/123/name-s", "name"),
        (
            "https://sqs.example/123",
            "-s",
            "name.fifo",
            "https://sqs.example/123/name-s.fifo",
            "name.fifo",
        ),
        (
            "https://sqs.example/123",
            "-s",
            "name-s-s.fifo",
            "https://sqs.example/123/name-s.fifo",
            "name.fifo",
        ),
        (
            "https://sqs.example/123/",
            "",
            "name.fifo",
            "https://sqs.example/123/name.fifo",
            "name.fifo",
        ),
        ("https://sqs.example/123", "", "name", "https://sqs.example/123/name", "name"),
        (
            "https://sqs.example/123",
            "-s",
            "https://sqs.example/123/name-s-s",
            "https://sqs.example/123/name-s-s",
            "name-s",
        ),
        (
            "https://sqs.example/123",
            "-s",
            "https://other/456/name-s.fifo",
            "https://other/456/name-s.fifo",
            "https://other/456/name.fifo",
        ),
        ("https://sqs.example/123", ".+", "name.+.+", "https://sqs.example/123/name.+", "name"),
        ("https://sqs.example/123", "-s", "", "https://sqs.example/123/default-s", "default"),
    ],
)
def test_queue_names(prefix, suffix, name, expected, logical):
    """Queue/SqsQueue.php:687,703; Support/Str.php:496; Foundation/Cloud/Queue.php:559."""
    connection = SqsConnectionConfig(
        prefix=prefix, suffix=suffix, region="us-east-1", credentials="ecs"
    )
    assert queue_url(connection, name) == expected
    assert normalize_queue(connection, expected) == logical


@pytest.mark.parametrize(
    ("name", "expected"),
    [("name-s", "name"), ("name-s-s", "name-s"), ("name-s.fifo", "name.fifo"), ("plain", "plain")],
)
def test_normalization_chops_suffix_once(name, expected):
    """Foundation/Cloud/Queue.php:566-569: chopEnd removes one suffix."""
    connection = SqsConnectionConfig(
        prefix="https://sqs.example/123", suffix="-s", region="us-east-1", credentials="ecs"
    )
    assert normalize_queue(connection, name) == expected
