"""Laravel Cloud queue event builders (D1, D6b).

Wire shape follows ``Illuminate\\Foundation\\Cloud\\Queue`` and
``FailedJobProvider::log``. The D1 size policy is measured on the encoded NDJSON
line, not on the raw PHP/Python string lengths. Symfony's payload projection is
intentionally not applied.
"""

from __future__ import annotations

import json
import secrets
import traceback
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Final, Literal

LifecycleType = Literal["queued", "started", "processed", "released", "failed"]
FAILED_JOB_LINE_LIMIT: Final = 16_384
"""Collector line limit documented by symfony-on-cloud (unverified; D1)."""
EXCEPTION_PREVIEW_LIMIT: Final = 1001

_DURATION_TYPES: Final = frozenset({"processed", "released", "failed"})
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)


def format_timestamp(moment: datetime) -> str:
    """UTC ``Y-m-d H:i:s.u`` (six-digit microseconds, no ``T``, no zone)."""

    if moment.tzinfo is not None and moment.tzinfo.utcoffset(moment) is not None:
        moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")


def _replace_lone_surrogates(text: str) -> str:
    """Replace UTF-16 surrogates with U+FFFD (PHP ``JSON_INVALID_UTF8_SUBSTITUTE``)."""

    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return "".join("\ufffd" if 0xD800 <= ord(char) <= 0xDFFF else char for char in text)
    return text


def _sanitize(value: object) -> object:
    if isinstance(value, str):
        return _replace_lone_surrogates(value)
    if isinstance(value, Mapping):
        cleaned: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("event keys must be strings")
            cleaned[_replace_lone_surrogates(key)] = _sanitize(item)
        return cleaned
    if isinstance(value, list | tuple):
        return [_sanitize(item) for item in value]
    return value


def encode_event_line(event: Mapping[str, object]) -> bytes:
    """Compact JSON, slashes and Unicode unescaped, zero fractions kept, invalid UTF-8 /
    lone surrogates replaced with U+FFFD, trailing newline."""

    payload = json.dumps(
        _sanitize(event),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return payload.encode("utf-8") + b"\n"


def lifecycle_event(
    type_: LifecycleType,
    queue: str,
    *,
    timestamp: datetime,
    duration_ms: int | None = None,
) -> dict[str, object]:
    """``duration_ms`` (truncated, >= 0) only for processed/released/failed."""

    event: dict[str, object] = {
        "_cloud_event": "queue",
        "timestamp": format_timestamp(timestamp),
        "type": type_,
        "queue": queue,
    }
    if type_ in _DURATION_TYPES:
        elapsed = 0 if duration_ms is None else duration_ms
        event["duration_ms"] = elapsed if elapsed > 0 else 0
    return event


def uuid7(timestamp: datetime) -> str:
    """RFC 9562 UUIDv7 bound to ``timestamp`` (millisecond precision)."""

    moment = timestamp
    if moment.tzinfo is None or moment.tzinfo.utcoffset(moment) is None:
        moment = moment.replace(tzinfo=timezone.utc)
    else:
        moment = moment.astimezone(timezone.utc)
    delta = moment - _EPOCH
    unix_ms = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
    raw = bytearray((unix_ms & 0xFFFFFFFFFFFF).to_bytes(6, "big"))
    raw.extend(secrets.token_bytes(10))
    raw[6] = (raw[6] & 0x0F) | 0x70
    raw[8] = (raw[8] & 0x3F) | 0x80
    hexed = raw.hex()
    return f"{hexed[:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:]}"


def _qualified_exception_name(exc: BaseException) -> str:
    cls = type(exc)
    if cls.__module__ == "builtins":
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"


def _exception_message(exc: BaseException) -> str:
    if exc.args and isinstance(exc.args[0], str):
        return exc.args[0]
    if not exc.args:
        return ""
    try:
        return str(exc)
    except Exception:
        return ""


def _exception_origin(exc: BaseException) -> tuple[str, int]:
    frame = exc.__traceback__
    if frame is None:
        return "unknown", 0
    while frame.tb_next is not None:
        frame = frame.tb_next
    return frame.tb_frame.f_code.co_filename, frame.tb_lineno


def _exception_preview(exc: BaseException) -> str:
    filename, lineno = _exception_origin(exc)
    name = _qualified_exception_name(exc)
    message = _exception_message(exc)
    if message:
        text = f"{name}: {message} in {filename}:{lineno}"
    else:
        text = f"{name} in {filename}:{lineno}"
    return text[:EXCEPTION_PREVIEW_LIMIT]


def _format_exception(exc: BaseException) -> str:
    try:
        lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    except Exception:
        return f"{_qualified_exception_name(exc)}: {_exception_message(exc)}\n"
    return "".join(lines)


def _job_name(payload: str) -> str:
    try:
        decoded = json.loads(payload)
    except ValueError:
        return ""
    if not isinstance(decoded, dict):
        return ""
    name = decoded.get("displayName")
    if isinstance(name, str):
        return name
    return ""


def _with_truncation_marker(original: str, head: str) -> str:
    removed = len(original.encode("utf-8")) - len(head.encode("utf-8"))
    if removed <= 0:
        return head
    return f"{head}\n... [truncated {removed} bytes]"


def _encoded_length(event: Mapping[str, object]) -> int:
    return len(encode_event_line(event))


def _largest_fitting_prefix(
    text: str,
    limit: int,
    render: Callable[[str], Mapping[str, object]],
) -> int | None:
    """Most code points of ``text`` whose rendered event encodes within ``limit``.

    Returns None when even an empty head does not fit. The search is measured on
    encoded bytes, so JSON escaping and multibyte characters count toward the limit.
    """

    def fits(head: str) -> bool:
        return _encoded_length(render(head)) <= limit

    if not fits(""):
        return None
    low = 0
    high = len(text)
    best = 0
    while low <= high:
        mid = (low + high) // 2
        if fits(text[:mid]):
            best = mid
            low = mid + 1
        else:
            high = mid - 1
    while best < len(text) and fits(text[: best + 1]):
        best += 1
    return best


def failed_job_event(
    *,
    queue: str,
    payload: str,
    exception: BaseException,
    attempts: int,
    started_at: datetime,
    timestamp: datetime,
    limit_bytes: int = FAILED_JOB_LINE_LIMIT,
) -> dict[str, object]:
    """Laravel ``FailedJobProvider::log`` field set; ``id`` = UUIDv7 from ``timestamp``;
    ``job_name`` from the payload's ``displayName`` (``""`` if unavailable). D1 size policy:
    whole line fits -> as is; else trim ``exception`` (head + marker); else also trim
    ``payload`` and add ``"replayable": false``. Measured on the encoded line in bytes."""

    limit = limit_bytes if limit_bytes > 0 else 0
    job_id = uuid7(timestamp)
    started = format_timestamp(started_at)
    preview = _exception_preview(exception)
    name = _job_name(payload)
    exception_text = _format_exception(exception)

    def assemble(body: str, exc_text: str, *, not_replayable: bool) -> dict[str, object]:
        event: dict[str, object] = {
            "_cloud_event": "failed_job",
            "id": job_id,
            "queue": queue,
            "started_at": started,
            "attempts": attempts,
            "payload": body,
            "exception_preview": preview,
            "job_name": name,
            "exception": exc_text,
        }
        if not_replayable:
            event["replayable"] = False
        return event

    full = assemble(payload, exception_text, not_replayable=False)
    if _encoded_length(full) <= limit:
        return full

    def trimmed_exception(head: str) -> str:
        return _with_truncation_marker(exception_text, head)

    # Phase 1: keep the payload and shorten the exception until the line fits.
    best_exception = _largest_fitting_prefix(
        exception_text,
        limit,
        lambda head: assemble(payload, trimmed_exception(head), not_replayable=False),
    )
    if best_exception is not None:
        return assemble(
            payload,
            trimmed_exception(exception_text[:best_exception]),
            not_replayable=False,
        )

    # The truncation marker can be larger than a tiny exception. An empty exception
    # is the shortest phase-1 result and still leaves the payload replayable.
    if _encoded_length(assemble(payload, "", not_replayable=False)) <= limit:
        return assemble(payload, "", not_replayable=False)

    # Phase 2: exception is exhausted. Trim the payload and mark it not replayable.
    marker = trimmed_exception("")

    def fit_payload(exc_text: str) -> int | None:
        return _largest_fitting_prefix(
            payload,
            limit,
            lambda head: assemble(head, exc_text, not_replayable=True),
        )

    if marker:
        best_payload = fit_payload(marker)
        if best_payload is not None:
            return assemble(payload[:best_payload], marker, not_replayable=True)

    best_payload = fit_payload("")
    if best_payload is not None:
        return assemble(payload[:best_payload], "", not_replayable=True)

    # Fixed fields alone exceed the limit. Return the smallest record we can build.
    return assemble("", "", not_replayable=True)


def failure_log_record(
    *,
    queue: str,
    payload: str,
    exception: BaseException,
    attempts: int,
    message_id: str,
    started_at: datetime,
    timestamp: datetime,
) -> dict[str, object]:
    """D6b log-only failure record for ``sqs``/``redis`` modes (one JSON line on stdout).

    The payload is included in full. D1's 16 KiB trim is not applied: these lines go to
    the worker's own stdout, not the Cloud failed-job collector. There is no receipt
    handle field; pass the broker message id as ``message_id``. Job arguments should
    not contain secrets.
    """

    return {
        "laravel_cloud_queues": "failed_job",
        "queue": queue,
        "message_id": message_id,
        "attempts": attempts,
        "job_name": _job_name(payload),
        "started_at": format_timestamp(started_at),
        "failed_at": format_timestamp(timestamp),
        "exception_preview": _exception_preview(exception),
        "exception": _format_exception(exception),
        "payload": payload,
    }
