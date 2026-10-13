"""Exercise encrypted Cloud endpoints and dispatch/event ordering."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from laravel_cloud_queues import Registry
from laravel_cloud_queues.config import QueueConfig, RedisConfig
from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.failed_jobs import _retry
from laravel_cloud_queues.jobs.envelope import Envelope, decode_envelope, encode_envelope
from laravel_cloud_queues.jobs.policy import RetryPolicy
from laravel_cloud_queues.observability import Telemetry
from laravel_cloud_queues.observability._events import retried_job_event
from laravel_cloud_queues.transports import SentMessage

from .conftest import encrypt

_URL = "https://cloud.test/failures?signature=secret"


@pytest.fixture
def registry() -> Registry:
    producer = Mock(supports_fifo=True, max_payload_bytes=1_048_576)
    producer.send.side_effect = lambda message: SentMessage("new-id", message.queue)
    sink = Mock()
    registry = Registry(
        config=QueueConfig(mode="redis", redis=RedisConfig(url="redis://unused")),
        backend=SimpleNamespace(producer=producer),
        telemetry=Telemetry(sink=sink, emits_cloud_events=True),
    )

    @registry.job(name="test.job", tries=99)
    def job(value: int) -> None:
        raise AssertionError("Retry must never run the handler")

    return registry


def record(**overrides: object) -> dict[str, object]:
    payload = encode_envelope(
        Envelope(
            uuid="original-uuid",
            display_name="test.job",
            job="test.job",
            args=(7,),
            policy=RetryPolicy(tries=3, backoff=(1, 5)),
            extra={"top_level": {"attempts": 4}},
        )
    )
    return {
        "id": "failed-id",
        "connection": "cloud",
        "queue": "emails",
        "payload": payload,
        **overrides,
    }


def endpoint(monkeypatch: pytest.MonkeyPatch, handler: object) -> list[httpx.Request]:
    requests = []
    client_type = httpx.Client

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    monkeypatch.setattr(
        _retry.httpx,
        "Client",
        lambda **kwargs: client_type(
            transport=httpx.MockTransport(handle),
            **kwargs,
        ),
    )
    return requests


def run(registry: Registry, urls: tuple[str, ...] = (_URL,)) -> tuple[int, list[Exception]]:
    errors = []
    code = _retry.retry_failed_jobs(
        registry, urls, cipher="AES-256-CBC", report_error=errors.append
    )
    return code, errors


def test_legacy_retry_dispatches_and_emits_in_order(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = endpoint(
        monkeypatch, lambda _: httpx.Response(200, content=encrypt(json.dumps(record())))
    )
    assert run(registry) == (0, [])
    sent = registry.backend.producer.send.call_args.args[0]
    decoded = decode_envelope(sent.body)
    assert decoded.args == (7,)
    assert decoded.policy == RetryPolicy(tries=3, backoff=(1, 5))
    assert decoded.uuid != "original-uuid"
    assert decoded.extra == {"top_level": {}, "section": {}, "policy": {}}
    assert sent.queue == "emails"
    events = [call.args[0] for call in registry.telemetry._sink.emit.call_args_list]
    assert events[0]["type"] == "queued"
    assert events[1] == {
        "_cloud_event": "failed_job",
        "id": "failed-id",
        "queue": "emails",
        "retried_at": events[1]["retried_at"],
    }
    assert datetime.strptime(events[1]["retried_at"], "%Y-%m-%d %H:%M:%S.%f")
    assert requests[0].headers["Cloud-Payload-Version"] == "1"
    assert requests[0].headers["Cloud-Encryption-Cipher"] == "AES-256-CBC"


def test_v1_pagination_skips_bad_job_and_continues_other_urls(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        data = [record(), record(payload="bad")]
        next_url = "https://cloud.test/page2" if request.url.path == "/failures" else None
        return httpx.Response(
            200,
            headers={"Cloud-Payload-Version": "1"},
            content=encrypt(
                json.dumps({"data": data, "links": {"self": str(request.url), "next": next_url}}),
            ),
        )

    requests = endpoint(monkeypatch, handle)
    code, errors = run(registry, (_URL, "https://cloud.test/other"))
    assert code == 1 and len(errors) == 3
    assert len(requests) == 3
    assert registry.backend.producer.send.call_count == 3


@pytest.mark.parametrize("status", [403, 404, 302])
def test_http_non_transient_errors_are_not_retried(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    requests = endpoint(monkeypatch, lambda _: httpx.Response(status))
    sleep = Mock()
    monkeypatch.setattr(_retry.time, "sleep", sleep)
    code, errors = run(registry)
    assert code == 1 and len(errors) == 1
    assert len(requests) == 1
    sleep.assert_not_called()
    registry.backend.producer.send.assert_not_called()


@pytest.mark.parametrize("failure", [500, 429, 408, "transport"])
@pytest.mark.parametrize("recover", [True, False])
def test_transient_errors_use_exponential_backoff(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
    failure: int | str,
    recover: bool,
) -> None:
    attempts = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if recover and attempts == 3:
            return httpx.Response(200, content=encrypt(json.dumps(record())))
        if failure == "transport":
            raise httpx.ConnectError("failed", request=request)
        return httpx.Response(failure)

    requests = endpoint(monkeypatch, handle)
    sleep = Mock()
    monkeypatch.setattr(_retry.time, "sleep", sleep)
    code, errors = run(registry)
    assert code == int(not recover)
    assert len(errors) == int(not recover)
    assert len(requests) == (3 if recover else 10)
    assert [call.args[0] for call in sleep.call_args_list] == [
        0.5 * 2**n for n in range(2 if recover else 9)
    ]


@pytest.mark.parametrize(
    "url",
    [
        "http://cloud.test/job",
        "file:///job",
        "https:///job",
        "https://user:password@cloud.test/job",
        "https://:password@cloud.test/job",
        "https://[invalid",
        "https://cloud.test:bad/job",
    ],
)
def test_non_https_or_invalid_urls_fail_before_any_dispatch(registry: Registry, url: str) -> None:
    with pytest.raises(ConfigurationError, match="HTTPS"):
        run(registry, (_URL, url))
    registry.backend.producer.send.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "version"),
    [
        ([], ""),
        ({}, "2"),
        ({"data": {}, "links": {}}, "1"),
        ({"data": [], "links": {}}, "1"),
        ({"data": [], "links": {"next": 4}}, "1"),
    ],
)
def test_invalid_page_shape_is_reported(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
    payload: object,
    version: str,
) -> None:
    endpoint(
        monkeypatch,
        lambda _: httpx.Response(
            200, headers={"Cloud-Payload-Version": version}, content=encrypt(json.dumps(payload))
        ),
    )
    code, errors = run(registry)
    # Empty pages are valid, including no next link.
    assert code == (0 if payload == {"data": [], "links": {}} else 1)
    assert len(errors) == code


@pytest.mark.parametrize("next_url", [_URL, "http://cloud.test/page2"])
def test_pagination_cycle_or_insecure_link_is_reported(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
    next_url: str,
) -> None:
    requests = endpoint(
        monkeypatch,
        lambda _: httpx.Response(
            200,
            headers={"Cloud-Payload-Version": "1"},
            content=encrypt(
                json.dumps(
                    {
                        "data": [],
                        "links": {"next": next_url},
                    }
                )
            ),
        ),
    )
    code, errors = run(registry)
    assert code == 1 and len(errors) == 1 and len(requests) == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"id": ""},
        {"id": None},
        {"queue": ""},
        {"payload": None},
        {"connection": "sqs"},
        {"payload": "bad"},
    ],
)
def test_bad_job_does_not_emit_retry_update(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, object],
) -> None:
    endpoint(
        monkeypatch, lambda _: httpx.Response(200, content=encrypt(json.dumps(record(**overrides))))
    )
    assert run(registry)[0] == 1
    registry.backend.producer.send.assert_not_called()
    registry.telemetry._sink.emit.assert_not_called()


def test_invalid_record_type_is_skipped(
    registry: Registry, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint(
        monkeypatch,
        lambda _: httpx.Response(
            200,
            headers={"Cloud-Payload-Version": "1"},
            content=encrypt(
                json.dumps(
                    {
                        "data": [None, record()],
                        "links": {"next": None},
                    }
                )
            ),
        ),
    )
    assert run(registry)[0] == 1
    assert registry.backend.producer.send.call_count == 1


def test_send_failure_does_not_emit_retry_update(
    registry: Registry, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint(monkeypatch, lambda _: httpx.Response(200, content=encrypt(json.dumps(record()))))
    registry.backend.producer.send.side_effect = RuntimeError("send failed")
    assert run(registry)[0] == 1
    registry.telemetry._sink.emit.assert_not_called()


def test_fifo_uses_pipeline_defaults(registry: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint(
        monkeypatch,
        lambda _: httpx.Response(200, content=encrypt(json.dumps(record(queue="jobs.fifo")))),
    )
    assert run(registry) == (0, [])
    sent = registry.backend.producer.send.call_args.args[0]
    assert sent.fifo_group == "jobs.fifo" and sent.deduplication_id


def test_telemetry_failure_does_not_fail_sent_job(
    registry: Registry, monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint(monkeypatch, lambda _: httpx.Response(200, content=encrypt(json.dumps(record()))))
    monkeypatch.setattr(
        registry.telemetry, "emit", Mock(side_effect=RuntimeError("telemetry failed"))
    )
    assert run(registry) == (0, [])
    assert registry.backend.producer.send.call_count == 1


def test_self_managed_mode_emits_no_cloud_events(
    registry: Registry, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = Mock()
    registry._telemetry = Telemetry(sink=sink, emits_cloud_events=False)
    endpoint(monkeypatch, lambda _: httpx.Response(200, content=encrypt(json.dumps(record()))))
    assert run(registry) == (0, [])
    sink.emit.assert_not_called()


def test_response_limit_is_enforced(registry: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_retry, "_MAX_RESPONSE_BYTES", 5)
    endpoint(monkeypatch, lambda _: httpx.Response(200, content=b"oversized"))
    assert run(registry)[0] == 1


def test_bad_mac_from_endpoint_never_dispatches(
    registry: Registry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint(
        monkeypatch,
        lambda _: httpx.Response(
            200,
            content=encrypt(json.dumps(record()), key=b"a" * 32),
        ),
    )
    code, errors = run(registry)
    assert code == 1 and len(errors) == 1
    assert "authenticated" in str(errors[0])
    registry.backend.producer.send.assert_not_called()
    registry.telemetry._sink.emit.assert_not_called()


def test_retry_timestamp_is_utc() -> None:
    assert retried_job_event(
        "id", "queue", timestamp=datetime(2026, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
    ) == {
        "_cloud_event": "failed_job",
        "id": "id",
        "queue": "queue",
        "retried_at": "2026-01-02 03:04:05.000006",
    }
