"""Timestamps, NDJSON encoding, lifecycle events, failed_job (D1), and D6b records."""

from __future__ import annotations

import json
import traceback
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest

from laravel_cloud_queues.observability import (
    EXCEPTION_PREVIEW_LIMIT,
    FAILED_JOB_LINE_LIMIT,
    encode_event_line,
    failed_job_event,
    failure_log_record,
    format_timestamp,
    lifecycle_event,
    uuid7,
)

_TS = datetime(2026, 9, 27, 12, 0, 0, 123456, tzinfo=UTC)
_STARTED = datetime(2026, 9, 27, 11, 59, 59, tzinfo=UTC)


class SampleError(Exception):
    """Local exception type so preview assertions can name the qualified class."""


def _raise(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:
        return caught


def _origin(exc: BaseException) -> tuple[str, int]:
    frame = exc.__traceback__
    if frame is None:
        return "unknown", 0
    while frame.tb_next is not None:
        frame = frame.tb_next
    return frame.tb_frame.f_code.co_filename, frame.tb_lineno


def _qualified(exc: BaseException) -> str:
    cls = type(exc)
    if cls.__module__ == "builtins":
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"


def _expected_preview(exc: BaseException) -> str:
    """Laravel ``FailedJobProvider`` preview, truncated to 1001 code points.

    ``src/Illuminate/Foundation/Cloud/FailedJobProvider.php:77-84``.
    """

    filename, lineno = _origin(exc)
    message = exc.args[0] if exc.args and isinstance(exc.args[0], str) else ""
    name = _qualified(exc)
    if message:
        text = f"{name}: {message} in {filename}:{lineno}"
    else:
        text = f"{name} in {filename}:{lineno}"
    return text[:EXCEPTION_PREVIEW_LIMIT]


def _exception_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def _failed(
    exc: BaseException,
    payload: str = '{"displayName":"SendEmail"}',
    *,
    limit_bytes: int = FAILED_JOB_LINE_LIMIT,
    queue: str = "emails",
    attempts: int = 2,
) -> dict[str, object]:
    return failed_job_event(
        queue=queue,
        payload=payload,
        exception=exc,
        attempts=attempts,
        started_at=_STARTED,
        timestamp=_TS,
        limit_bytes=limit_bytes,
    )


def test_format_timestamp_is_utc_without_separator() -> None:
    """UTC ``Y-m-d H:i:s.u``.

    Conformance: ``toDateTimeString('microsecond')`` in
    ``src/Illuminate/Foundation/Cloud/Queue.php:483`` and ``:522``.
    Naive datetimes are treated as UTC; aware datetimes are converted.
    """

    assert format_timestamp(_TS) == "2026-09-27 12:00:00.123456"
    assert format_timestamp(datetime(2026, 9, 27, 12, 0, 0)) == "2026-09-27 12:00:00.000000"
    offset = timezone(timedelta(hours=5, minutes=30))
    shifted = datetime(2026, 9, 27, 17, 30, 0, 1, tzinfo=offset)
    assert format_timestamp(shifted) == "2026-09-27 12:00:00.000001"


def test_encode_event_line_matches_laravel_json_flags() -> None:
    """Compact UTF-8 NDJSON: unescaped slashes and Unicode, preserved zero fractions.

    Conformance: ``src/Illuminate/Foundation/Cloud/Events.php:124``
    (``JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION |
    JSON_INVALID_UTF8_SUBSTITUTE``).
    """

    line = encode_event_line({"path": "a/b", "name": "café", "n": 1.0, "z": 0.0, "ok": True})
    assert line.endswith(b"\n")
    assert line.count(b"\n") == 1
    assert b"a/b" in line
    assert b"\\/" not in line
    assert "café".encode() in line
    assert b"\\u00e9" not in line
    assert b"1.0" in line
    assert b"0.0" in line
    assert b" " not in line
    assert json.loads(line) == {"path": "a/b", "name": "café", "n": 1.0, "z": 0.0, "ok": True}


def test_encode_event_line_replaces_lone_surrogates_and_rejects_nan() -> None:
    line = encode_event_line({"s": "A\ud800B"})
    assert json.loads(line) == {"s": "A\ufffdB"}
    framed = encode_event_line({"exception": "top\nbottom"})
    assert framed.count(b"\n") == 1
    assert json.loads(framed)["exception"] == "top\nbottom"

    with pytest.raises(ValueError):
        encode_event_line({"n": float("nan")})
    with pytest.raises(ValueError):
        encode_event_line({"n": float("inf")})


def test_lifecycle_key_order_and_duration() -> None:
    """Lifecycle key order matches Laravel. ``duration_ms`` is only on completion events.

    Conformance: ``src/Illuminate/Foundation/Cloud/Queue.php:520-525`` (queued),
    ``:545-550`` (started), ``:481-491`` (processed/released/failed).
    ``duration_ms`` is ``(int) diffInMilliseconds`` at ``:490``; values are non-negative.
    """

    for event_type in ("queued", "started"):
        event = lifecycle_event(event_type, "emails", timestamp=_TS, duration_ms=5)
        assert list(event) == ["_cloud_event", "timestamp", "type", "queue"]
        assert "duration_ms" not in event
        assert event["type"] == event_type
        assert event["_cloud_event"] == "queue"
        assert event["timestamp"] == "2026-09-27 12:00:00.123456"

    for event_type in ("processed", "released", "failed"):
        event = lifecycle_event(event_type, "emails", timestamp=_TS, duration_ms=42)
        assert list(event) == ["_cloud_event", "timestamp", "type", "queue", "duration_ms"]
        assert event["duration_ms"] == 42

    clamped = lifecycle_event("processed", "emails", timestamp=_TS, duration_ms=-5)
    assert clamped["duration_ms"] == 0
    missing = lifecycle_event("failed", "emails", timestamp=_TS)
    assert missing["duration_ms"] == 0
    raw = encode_event_line(lifecycle_event("processed", "emails", timestamp=_TS, duration_ms=42))
    assert raw.startswith(
        b'{"_cloud_event":"queue","timestamp":"2026-09-27 12:00:00.123456",'
        b'"type":"processed","queue":"emails","duration_ms":42}\n'
    )


def test_uuid7_embeds_timestamp_version_and_variant() -> None:
    """UUIDv7 bound to the given millisecond, version 7, RFC 4122 variant.

    Laravel calls ``Str::uuid7($timestamp)`` at
    ``src/Illuminate/Foundation/Cloud/FailedJobProvider.php:72``
    (``src/Illuminate/Support/Str.php:2094``). stdlib ``uuid.uuid7`` cannot take a
    timestamp (D13.6), so this is an in-package RFC 9562 layout.
    """

    first = uuid.UUID(uuid7(_TS))
    second = uuid.UUID(uuid7(_TS))
    naive = uuid.UUID(uuid7(datetime(2026, 9, 27, 12, 0, 0, 123456)))
    delta = _TS - datetime(1970, 1, 1, tzinfo=UTC)
    unix_ms = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    assert first.version == 7
    assert first.variant is uuid.RFC_4122
    assert first.int >> 80 == unix_ms
    assert second.int >> 80 == unix_ms
    assert naive.int >> 80 == unix_ms
    assert first.int != second.int
    assert str(first) == str(first).lower()
    assert len(str(first)) == 36


def test_failed_job_fields_preview_and_key_order() -> None:
    """``failed_job`` field set and order from ``FailedJobProvider::log``.

    Conformance: ``src/Illuminate/Foundation/Cloud/FailedJobProvider.php:70-87``.
    ``job_name`` is the payload ``displayName`` or ``""``. The exception text is the
    Python traceback (the analog of PHP ``(string) $exception``).
    """

    exc = _raise(SampleError("smtp down"))
    payload = '{"uuid":"job-1","displayName":"SendEmail","body":"a/b"}'
    event = _failed(exc, payload, attempts=3)
    assert list(event) == [
        "_cloud_event",
        "id",
        "queue",
        "started_at",
        "attempts",
        "payload",
        "exception_preview",
        "job_name",
        "exception",
    ]
    assert event["_cloud_event"] == "failed_job"
    assert event["queue"] == "emails"
    assert event["started_at"] == "2026-09-27 11:59:59.000000"
    assert event["attempts"] == 3
    assert event["payload"] == payload
    assert event["job_name"] == "SendEmail"
    assert event["exception_preview"] == _expected_preview(exc)
    assert str(event["exception_preview"]).startswith(f"{__name__}.SampleError: smtp down in ")
    parsed_id = uuid.UUID(str(event["id"]))
    assert parsed_id.version == 7
    assert parsed_id.int >> 80 == uuid.UUID(uuid7(_TS)).int >> 80
    exception_text = str(event["exception"])
    assert "Traceback (most recent call last):" in exception_text
    assert "smtp down" in exception_text
    encoded = encode_event_line(event)
    assert b"\\/" not in encoded
    assert json.loads(encoded)["job_name"] == "SendEmail"


def test_failed_job_preview_without_message_and_without_traceback() -> None:
    raised = _raise(SampleError())
    preview = str(_failed(raised)["exception_preview"])
    assert preview == _expected_preview(raised)
    assert ": " not in preview.split(" in ", 1)[0]

    unraised = SampleError("plain")
    preview = str(_failed(unraised)["exception_preview"])
    assert preview == f"{__name__}.SampleError: plain in unknown:0"
    assert "plain" in str(_failed(unraised)["exception"])


def test_failed_job_preview_caps_at_1001_code_points() -> None:
    exc = _raise(SampleError("é" * 2000))
    preview = str(_failed(exc)["exception_preview"])
    assert preview == _expected_preview(exc)
    assert len(preview) == EXCEPTION_PREVIEW_LIMIT
    assert "é" in preview
    builtin = _raise(ValueError("nope"))
    builtin_preview = str(_failed(builtin)["exception_preview"])
    assert builtin_preview.startswith("ValueError: nope in ")
    assert not builtin_preview.startswith("builtins.")


def test_failed_job_name_falls_back_to_empty_string() -> None:
    exc = _raise(SampleError("x"))
    assert _failed(exc, "not-json")["job_name"] == ""
    assert _failed(exc, "[1, 2]")["job_name"] == ""
    assert _failed(exc, '{"displayName": 5}')["job_name"] == ""
    assert _failed(exc, "{}")["job_name"] == ""


def test_failed_job_as_is_when_the_line_fits() -> None:
    """D1 branch 1: a line within the limit is not trimmed.

    The 16 KiB ceiling is the symfony-on-cloud collector note at
    ``src/Queue/QueueEventSubscriber.php:50-60``. Symfony's payload projection
    (``:202-219``) is not applied: the payload string is kept whole.
    """

    exc = _raise(SampleError("smtp down"))
    payload = '{"displayName":"SendEmail","body":"keep-me"}'
    event = _failed(exc, payload)
    assert event["payload"] == payload
    assert event["exception"] == _exception_text(exc)
    assert "replayable" not in event
    assert len(encode_event_line(event)) <= FAILED_JOB_LINE_LIMIT


def _marker(text: str) -> tuple[str, int]:
    head, separator, tail = text.partition("\n... [truncated ")
    assert separator
    assert tail.endswith(" bytes]")
    return head, int(tail[: -len(" bytes]")])


def test_d1_trims_exception_before_payload() -> None:
    """D1 branch 2: trim the exception (head + marker) and keep the payload replayable.

    Measured on the encoded line, including the trailing newline.
    """

    exc = _raise(SampleError("E" * 4000))
    payload = '{"displayName":"SendEmail","body":"intact"}'
    full = _failed(exc, payload, limit_bytes=10**9)
    full_len = len(encode_event_line(full))
    original = _exception_text(exc)

    as_is = _failed(exc, payload, limit_bytes=full_len)
    assert as_is["exception"] == original
    assert "replayable" not in as_is
    assert len(encode_event_line(as_is)) == full_len

    limit = full_len - 1
    trimmed = _failed(exc, payload, limit_bytes=limit)
    assert trimmed["payload"] == payload
    assert "replayable" not in trimmed
    assert list(trimmed) == list(full)
    encoded_len = len(encode_event_line(trimmed))
    assert encoded_len <= limit
    head, removed = _marker(str(trimmed["exception"]))
    assert original.startswith(head)
    assert removed == len(original.encode()) - len(head.encode())
    assert removed > 0
    longer_head = original[: len(head) + 1]
    longer_removed = len(original.encode()) - len(longer_head.encode())
    candidate = dict(trimmed)
    candidate["exception"] = f"{longer_head}\n... [truncated {longer_removed} bytes]"
    assert len(encode_event_line(candidate)) > limit


def test_d1_trims_payload_and_marks_not_replayable() -> None:
    """D1 branch 3: after the exception is exhausted, trim the payload and set replayable false."""

    exc = _raise(SampleError("boom " + ("\\" * 800)))
    payload = json.dumps(
        {"displayName": "SendEmail", "body": 'é"' * 4000},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    full = _failed(exc, payload, limit_bytes=10**9)
    empty_exception = dict(full)
    empty_exception["exception"] = ""
    empty_exception_len = len(encode_event_line(empty_exception))
    skeleton = dict(empty_exception)
    skeleton["payload"] = ""
    skeleton["replayable"] = False
    skeleton_len = len(encode_event_line(skeleton))
    limit = skeleton_len + 80
    assert limit < empty_exception_len

    result = _failed(exc, payload, limit_bytes=limit)
    assert len(encode_event_line(result)) <= limit
    assert result["replayable"] is False
    assert list(result)[-1] == "replayable"
    assert isinstance(result["payload"], str)
    body = result["payload"]
    assert payload.startswith(body)
    assert body != payload
    assert json.loads(encode_event_line(result))["replayable"] is False
    longer = dict(result)
    longer["payload"] = payload[: len(body) + 1]
    assert len(encode_event_line(longer)) > limit


def test_d1_respects_the_16kib_collector_limit() -> None:
    exc = _raise(SampleError("E" * 30_000))
    payload = json.dumps({"displayName": "SendEmail", "body": "p" * 30_000})
    result = _failed(exc, payload)
    assert len(encode_event_line(result)) <= FAILED_JOB_LINE_LIMIT
    if result["payload"] != payload:
        assert result["replayable"] is False
    else:
        assert "replayable" not in result
        assert "\n... [truncated " in str(result["exception"])


def test_failure_log_record_is_full_and_has_no_receipt_handle() -> None:
    """D6b stdout record. The payload is not D1-trimmed; receipt handles are not a field."""

    exc = _raise(SampleError("disk full"))
    payload = json.dumps({"displayName": "SendEmail", "body": "p" * 20_000})
    record = failure_log_record(
        queue="emails",
        payload=payload,
        exception=exc,
        attempts=4,
        message_id="msg-123",
        started_at=_STARTED,
        timestamp=_TS,
    )
    assert list(record) == [
        "laravel_cloud_queues",
        "queue",
        "message_id",
        "attempts",
        "job_name",
        "started_at",
        "failed_at",
        "exception_preview",
        "payload",
    ]
    assert record["laravel_cloud_queues"] == "failed_job"
    assert record["message_id"] == "msg-123"
    assert record["attempts"] == 4
    assert record["job_name"] == "SendEmail"
    assert record["payload"] == payload
    assert record["failed_at"] == "2026-09-27 12:00:00.123456"
    assert record["exception_preview"] == _expected_preview(exc)
    assert "receipt" not in record
    assert len(str(record["payload"])) > FAILED_JOB_LINE_LIMIT


def test_encode_event_line_sanitizes_nested_sequences_and_rejects_non_string_keys() -> None:
    line = encode_event_line({"items": ("A\ud800", ["B\udfff"]), "n": (1, 2)})
    assert json.loads(line) == {"items": ["A�", ["B�"]], "n": [1, 2]}
    with pytest.raises(TypeError, match="event keys must be strings"):
        encode_event_line({"outer": {1: "x"}})


class UnprintableError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("no string form")


def test_failed_job_message_falls_back_to_str_for_non_string_args() -> None:
    numeric = _raise(SampleError(404))
    assert str(_failed(numeric)["exception_preview"]).startswith(f"{__name__}.SampleError: 404 in ")
    unprintable = _raise(UnprintableError(404))
    preview = str(_failed(unprintable)["exception_preview"])
    assert preview.startswith(f"{__name__}.UnprintableError in ")
    assert ": " not in preview.split(" in ", 1)[0]


def test_failed_job_origin_is_the_innermost_frame() -> None:
    def inner() -> None:
        raise SampleError("deep")

    def outer() -> None:
        inner()

    try:
        outer()
    except SampleError as caught:
        exc = caught
    filename, lineno = _origin(exc)
    assert exc.__traceback__ is not None
    assert exc.__traceback__.tb_next is not None
    assert str(_failed(exc)["exception_preview"]) == (
        f"{__name__}.SampleError: deep in {filename}:{lineno}"
    )


def test_failed_job_exception_falls_back_when_the_traceback_cannot_be_formatted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def explode(*_args: object, **_kwargs: object) -> list[str]:
        raise RuntimeError("formatter down")

    monkeypatch.setattr(traceback, "format_exception", explode)
    exc = _raise(SampleError("smtp down"))
    assert _failed(exc)["exception"] == f"{__name__}.SampleError: smtp down\n"


def _without_id(event: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in event.items() if key != "id"}


def test_d1_keeps_the_payload_with_an_empty_exception_when_the_marker_does_not_fit() -> None:
    """The truncation marker can outweigh a tiny exception; an empty one still fits."""

    exc = _raise(SampleError("x"))
    payload = '{"displayName":"SendEmail","body":"intact"}'
    full = _failed(exc, payload, limit_bytes=10**9)
    emptied = dict(full)
    emptied["exception"] = ""
    limit = len(encode_event_line(emptied))

    result = _failed(exc, payload, limit_bytes=limit)
    assert _without_id(result) == _without_id(emptied)
    assert "replayable" not in result
    assert len(encode_event_line(result)) == limit


def test_d1_drops_the_marker_and_then_the_payload_when_nothing_else_fits() -> None:
    exc = _raise(SampleError("x"))
    payload = '{"displayName":"SendEmail","body":"long enough to be trimmed away"}'
    full = _failed(exc, payload, limit_bytes=10**9)
    skeleton = dict(full)
    skeleton["exception"] = ""
    skeleton["payload"] = ""
    skeleton["replayable"] = False
    skeleton_len = len(encode_event_line(skeleton))

    result = _failed(exc, payload, limit_bytes=skeleton_len + 5)
    assert result["replayable"] is False
    assert result["exception"] == ""
    assert payload.startswith(str(result["payload"]))
    assert 0 < len(str(result["payload"])) <= 5
    assert len(encode_event_line(result)) <= skeleton_len + 5

    for limit in (skeleton_len - 1, 0, -1):
        smallest = _failed(exc, payload, limit_bytes=limit)
        assert _without_id(smallest) == _without_id(skeleton)
