"""The builders for Laravel Cloud queue events and failure records.

The wire shape follows ``Illuminate\\Foundation\\Cloud\\Queue`` and
``FailedJobProvider::log``. The failed job size limit is measured on the encoded NDJSON
line, not on the raw PHP or Python string lengths. Symfony's payload projection is
intentionally not applied.
"""

from __future__ import annotations

import json
import secrets
import traceback
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Final, Literal

from typing_extensions import TypeIs

from .._narrowing import is_mapping

LifecycleType = Literal["queued", "started", "processed", "released", "failed"]
"""The lifecycle stages a queue event may report."""
FAILED_JOB_LINE_LIMIT: Final = 16_384
"""The maximum size in bytes of an encoded failed job line.

This is the collector limit documented by symfony-on-cloud and has not been verified.
"""
EXCEPTION_PREVIEW_LIMIT: Final = 1001
"""The maximum number of characters in an exception preview."""

_DURATION_TYPES: Final = frozenset({"processed", "released", "failed"})
"""The lifecycle stages that carry a duration."""
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
"""The Unix epoch as an aware UTC datetime."""


def format_timestamp(moment: datetime) -> str:
    """Format the given moment as a Laravel timestamp.

    The result is UTC ``Y-m-d H:i:s.u`` with six-digit microseconds, no ``T`` and no zone.
    Naive datetimes are assumed to already be in UTC.
    """

    if moment.tzinfo is not None and moment.tzinfo.utcoffset(moment) is not None:
        moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")


def _replace_lone_surrogates(text: str) -> str:
    """Replace lone UTF-16 surrogates with U+FFFD.

    This mirrors PHP's ``JSON_INVALID_UTF8_SUBSTITUTE`` flag.
    """

    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return "".join("\ufffd" if 0xD800 <= ord(char) <= 0xDFFF else char for char in text)
    return text


def _is_sequence(value: object) -> TypeIs[list[object] | tuple[object, ...]]:
    """Determine if the value is a list or a tuple."""
    return isinstance(value, list | tuple)


def sanitize(value: object) -> object:
    """Replace lone surrogates throughout the given value, recursively.

    Tuples become lists. Raises a :class:`TypeError` if a mapping has a non-string key.
    """
    if isinstance(value, str):
        return _replace_lone_surrogates(value)
    if is_mapping(value):
        cleaned: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("event keys must be strings")
            cleaned[_replace_lone_surrogates(key)] = sanitize(item)
        return cleaned
    if _is_sequence(value):
        return [sanitize(item) for item in value]
    return value


def encode_event_line(event: Mapping[str, object]) -> bytes:
    """Encode the given event as a single NDJSON line.

    The JSON is compact, leaves slashes and Unicode unescaped, keeps zero fractions,
    replaces invalid UTF-8 and lone surrogates with U+FFFD, and ends with a newline.
    """

    payload = json.dumps(
        sanitize(event),
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
    """Build a Cloud queue lifecycle event.

    Only processed, released and failed events carry ``duration_ms``, which is
    truncated to a whole number and never negative.
    """

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
    """Generate an RFC 9562 UUIDv7 for the given timestamp.

    The timestamp is embedded with millisecond precision; naive datetimes are treated
    as UTC.
    """

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
    """Get the qualified class name of the exception, omitting ``builtins``."""
    cls = type(exc)
    if cls.__module__ == "builtins":
        return cls.__qualname__
    return f"{cls.__module__}.{cls.__qualname__}"


def _exception_message(exc: BaseException) -> str:
    """Get the message of the exception, or an empty string if it has none."""
    if exc.args and isinstance(exc.args[0], str):
        return exc.args[0]
    if not exc.args:
        return ""
    try:
        return str(exc)
    except Exception:
        return ""


def _exception_origin(exc: BaseException) -> tuple[str, int]:
    """Get the file and line where the exception was raised."""
    frame = exc.__traceback__
    if frame is None:
        return "unknown", 0
    while frame.tb_next is not None:
        frame = frame.tb_next
    return frame.tb_frame.f_code.co_filename, frame.tb_lineno


def _exception_preview(exc: BaseException) -> str:
    """Build the one-line exception preview, in Laravel's ``Name: message in file:line`` form."""
    filename, lineno = _exception_origin(exc)
    name = _qualified_exception_name(exc)
    message = _exception_message(exc)
    if message:
        text = f"{name}: {message} in {filename}:{lineno}"
    else:
        text = f"{name} in {filename}:{lineno}"
    return text[:EXCEPTION_PREVIEW_LIMIT]


def _format_exception(exc: BaseException) -> str:
    """Format the full traceback of the exception, falling back to its name and message."""
    try:
        lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    except Exception:
        return f"{_qualified_exception_name(exc)}: {_exception_message(exc)}\n"
    return "".join(lines)


def _job_name(payload: str) -> str:
    """Get the ``displayName`` from the payload, or an empty string if it is unavailable."""
    try:
        decoded: object = json.loads(payload)
    except ValueError:
        return ""
    if not is_mapping(decoded):
        return ""
    name = decoded.get("displayName")
    if isinstance(name, str):
        return name
    return ""


def _with_truncation_marker(original: str, head: str) -> str:
    """Append a marker to the head noting how many bytes of the original were removed."""
    removed = len(original.encode("utf-8")) - len(head.encode("utf-8"))
    return f"{head}\n... [truncated {removed} bytes]"


def _encoded_length(event: Mapping[str, object]) -> int:
    """Get the size in bytes of the event's encoded line."""
    return len(encode_event_line(event))


def _largest_fitting_prefix(
    text: str,
    limit: int,
    render: Callable[[str], Mapping[str, object]],
) -> int | None:
    """Find the longest prefix of the text whose rendered event fits within the limit.

    The result is a number of code points, or None when even an empty head does not fit.
    The search is measured on encoded bytes, so JSON escaping and multibyte characters
    count toward the limit. The encoded size never shrinks as the head grows (each code
    point adds at least one byte and a truncation marker loses at most one digit), so the
    binary search is exact.
    """

    def fits(head: str) -> bool:
        """Determine if the event rendered with the given head fits within the limit."""
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
    """Build a Cloud failed job event with Laravel's ``FailedJobProvider::log`` fields.

    The ``id`` is a UUIDv7 derived from ``timestamp`` and ``job_name`` is the payload's
    ``displayName``, or ``""`` if unavailable. If the encoded line exceeds ``limit_bytes``,
    the exception is trimmed to a head plus a truncation marker; if that is not enough, the
    payload is trimmed as well and the event is marked with ``"replayable": false``.
    """

    limit = limit_bytes if limit_bytes > 0 else 0
    job_id = uuid7(timestamp)
    started = format_timestamp(started_at)
    preview = _exception_preview(exception)
    name = _job_name(payload)
    exception_text = _format_exception(exception)

    def assemble(body: str, exc_text: str, *, not_replayable: bool) -> dict[str, object]:
        """Build the event with the given payload and exception text."""
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
        """Get the exception head with its truncation marker."""
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
        """Find the longest payload prefix that fits beside the given exception text."""
        return _largest_fitting_prefix(
            payload,
            limit,
            lambda head: assemble(head, exc_text, not_replayable=True),
        )

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
    """Build the failure record logged for each terminal failure, in every mode.

    The payload is included in full and the 16 KiB failed job limit is not applied, since
    these lines go to the worker's logs rather than the Cloud failed job collector. The
    exception is not included: :meth:`Telemetry.log_line` attaches it to the log line.
    There is no receipt handle field; pass the broker message id as ``message_id``. Job
    arguments should not contain secrets.
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
        "payload": payload,
    }
