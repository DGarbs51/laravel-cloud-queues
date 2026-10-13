"""Agent requests stay out of the logs; Laravel's worker logs nothing per agent call."""

import logging

import httpx
import pytest

from laravel_cloud_queues.config import AgentConfig, ManagedQueuesConfig, SqsConnectionConfig
from laravel_cloud_queues.transports import agent

MANAGED = ManagedQueuesConfig(
    connection=SqsConnectionConfig(
        prefix="https://sqs.us-east-1.amazonaws.com/123456789012",
        region="us-east-1",
        credentials="ecs",
    ),
    agent=AgentConfig(enabled=True, socket="/unused.sock"),
    queue="assigned",
)


@pytest.fixture
def httpx_records():
    logger = logging.getLogger("httpx")
    records: list[logging.LogRecord] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler, level = Collect(), logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        logger.removeFilter(agent._QUIET_AGENT_REQUESTS)


def client() -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(204)),
        base_url="http://localhost",
    )


def test_agent_requests_are_not_logged_but_other_requests_are(monkeypatch, httpx_records):
    monkeypatch.setattr(
        httpx, "HTTPTransport", lambda **kw: httpx.MockTransport(lambda r: httpx.Response(204))
    )
    consumer = agent.AgentConsumer(MANAGED)
    agent.AgentConsumer(MANAGED).close()  # a second consumer installs nothing new
    assert logging.getLogger("httpx").filters.count(agent._QUIET_AGENT_REQUESTS) == 1
    try:
        assert consumer.receive(["assigned"], 0) is None
        with client() as other:
            other.get("/result")
            other.get("/other")
            other.get("http://example.com/next")
    finally:
        consumer.close()
    # The consumer's GET /next and the app's own GET /result are both agent URLs and dropped.
    assert [str(record.args[1]) for record in httpx_records] == [
        "http://localhost/other",
        "http://example.com/next",
    ]


def test_async_consumer_installs_the_filter(monkeypatch):
    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport", lambda **kw: httpx.MockTransport(lambda r: httpx.Response(204))
    )
    logging.getLogger("httpx").removeFilter(agent._QUIET_AGENT_REQUESTS)
    agent.AsyncAgentConsumer(MANAGED)
    try:
        assert agent._QUIET_AGENT_REQUESTS in logging.getLogger("httpx").filters
    finally:
        logging.getLogger("httpx").removeFilter(agent._QUIET_AGENT_REQUESTS)


@pytest.mark.parametrize(
    ("level", "args", "kept"),
    [
        (logging.INFO, ("GET", "http://localhost/next", "HTTP/1.1", 204, "No Content"), False),
        (logging.WARNING, ("GET", "http://localhost/next", "HTTP/1.1", 204, "No Content"), True),
        (logging.INFO, ("GET",), True),
        (logging.INFO, None, True),
    ],
)
def test_filter_drops_only_info_lines_about_agent_urls(level, args, kept):
    record = logging.LogRecord("httpx", level, __file__, 1, "HTTP Request: %s", args, None)
    assert agent._QUIET_AGENT_REQUESTS.filter(record) is kept
