"""Envelope v1 (PROJECT_SCOPE.md §8). CONTRACT — implemented by lane L3b.

Wire shape (one JSON object; keys other than ``uuid``/``displayName`` live under the
versioned ``laravel_cloud_queues`` section)::

    {
      "uuid": "7f0c...-...",                 # unique per logical dispatch (Laravel key)
      "displayName": "emails.send",          # job wire name (Laravel key; failed_job job_name)
      "laravel_cloud_queues": {
        "version": 1,
        "job": "emails.send",                # registry lookup key (== displayName in v1)
        "args": [<encoded>, ...],
        "kwargs": {"name": <encoded>, ...},
        "policy": {"tries": 3, "backoff": [1, 5], "timeout": 60, "fail_on_timeout": false},
        "queue": "emails",                   # logical queue at dispatch (debugging only)
        "dispatched_at": "2026-09-27T12:00:00.123456+00:00",
        "context": {"traceparent": "...", "tracestate": "..."}
      }
    }

Rules: ``policy`` omits fields the job did not declare; reserved (not emitted in v1):
``retry_until``, ``max_exceptions``. Unknown keys inside the section and at top level are
preserved on decode (``extra``) and ignored. A re-queued envelope (dashboard retry, D3) is
decoded identically; attempts come from the transport, never from the body.
Decoding limits: ``MAX_BODY_BYTES`` total, ``MAX_DEPTH`` nesting; reject NaN/Infinity,
duplicate keys, non-object top level -> MalformedEnvelopeError. ``{"@pointer": ...}`` ->
UnsupportedOverflowPayloadError. ``version`` != 1 -> UnsupportedEnvelopeVersionError.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import NoReturn, cast

from ..codecs import JSONValue
from ..errors import (
    ConfigurationError,
    MalformedEnvelopeError,
    SerializationError,
    UnsupportedEnvelopeVersionError,
    UnsupportedOverflowPayloadError,
)
from .policy import RetryPolicy

ENVELOPE_KEY = "laravel_cloud_queues"
ENVELOPE_VERSION = 1
MAX_BODY_BYTES = 16 * 1_048_576
MAX_DEPTH = 64


@dataclass(frozen=True)
class Envelope:
    uuid: str
    display_name: str
    job: str
    args: tuple[JSONValue, ...] = ()
    kwargs: Mapping[str, JSONValue] = field(default_factory=dict)
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    queue: str | None = None
    dispatched_at: str | None = None
    context: Mapping[str, str] = field(default_factory=dict)
    extra: Mapping[str, JSONValue] = field(default_factory=dict)
    """Namespaced extras: ``top_level``, ``section`` and ``policy`` object maps.
    Unknown/reserved policy fields are retained but have no runtime effect.
    """


def encode_envelope(envelope: Envelope) -> str:
    """Deterministic compact JSON; omit undeclared policy fields and optional metadata."""
    try:
        top = _extras(envelope, "top_level")
        section = _extras(envelope, "section")
        policy = _extras(envelope, "policy")
        for name in ("tries", "backoff", "timeout", "fail_on_timeout"):
            value = getattr(envelope.policy, name)
            if value is not None:
                policy[name] = (
                    list(value)
                    if name == "backoff" and not isinstance(value, (int, float))
                    else value
                )
            else:
                policy.pop(name, None)
        section.update(
            version=ENVELOPE_VERSION,
            job=envelope.job,
            args=list(envelope.args),
            kwargs=dict(envelope.kwargs),
            policy=policy,
            context=dict(envelope.context),
        )
        for key, value in (("queue", envelope.queue), ("dispatched_at", envelope.dispatched_at)):
            if value is not None:
                section[key] = value
            else:
                section.pop(key, None)
        top.update(uuid=envelope.uuid, displayName=envelope.display_name)
        top[ENVELOPE_KEY] = section
        return json.dumps(
            top, separators=(",", ":"), ensure_ascii=False, allow_nan=False, sort_keys=True
        )
    except (ValueError, TypeError, RecursionError):
        raise SerializationError("Envelope cannot be serialized") from None


def _extras(envelope: Envelope, key: str) -> dict[str, JSONValue]:
    value = envelope.extra.get(key, {})
    if not isinstance(value, dict):
        raise ValueError("Envelope extras must be namespaced objects")
    return dict(value)


def _reject_constant(value: str) -> NoReturn:
    raise ValueError("Non-finite JSON number")


def _float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Non-finite JSON number")
    return result


def _pairs(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _parse(body: str) -> dict[str, JSONValue]:
    try:
        # Bound bytes before parsing, and nesting before the recursive stdlib parser.
        if len(body) > MAX_BODY_BYTES or len(body.encode("utf-8")) > MAX_BODY_BYTES:
            raise ValueError("Envelope exceeds body limit")
        depth = 0
        quoted = escaped = False
        for char in body:
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char in "[{":
                depth += 1
                if depth > MAX_DEPTH:
                    raise ValueError("Envelope exceeds nesting limit")
            elif char in "]}":
                depth -= 1
        value: object = json.loads(
            body, object_pairs_hook=_pairs, parse_constant=_reject_constant, parse_float=_float
        )
        if not isinstance(value, dict):
            raise ValueError("Envelope must be an object")
        return cast(dict[str, JSONValue], value)
    except (ValueError, TypeError, RecursionError):
        raise MalformedEnvelopeError("Invalid JSON envelope or decode limit exceeded") from None


def _number(value: JSONValue) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def decode_envelope(body: str) -> Envelope:
    """Strict bounded decode. Unknown/reserved fields are ignored and preserved."""
    top = _parse(body)
    if "@pointer" in top:
        raise UnsupportedOverflowPayloadError("Laravel overflow payloads are unsupported")
    section = top.get(ENVELOPE_KEY)
    if not isinstance(section, dict):
        raise MalformedEnvelopeError("Missing or invalid envelope section")
    version = section.get("version")
    if type(version) is not int:
        raise MalformedEnvelopeError("Envelope version must be an integer")
    if version != ENVELOPE_VERSION:
        raise UnsupportedEnvelopeVersionError(version)
    for obj, key in ((top, "uuid"), (top, "displayName"), (section, "job")):
        if not isinstance(obj.get(key), str):
            raise MalformedEnvelopeError(f"Envelope {key} must be a string")
    args, kwargs = section.get("args"), section.get("kwargs")
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise MalformedEnvelopeError("Envelope args/kwargs have invalid shapes")
    for key in ("queue", "dispatched_at"):
        if key in section and not isinstance(section[key], str):
            raise MalformedEnvelopeError(f"Envelope {key} must be a string")
    context = section.get("context", {})
    if not isinstance(context, dict) or any(not isinstance(v, str) for v in context.values()):
        raise MalformedEnvelopeError("Envelope context must contain string values")
    policy = section.get("policy", {})
    if not isinstance(policy, dict):
        raise MalformedEnvelopeError("Envelope policy must be an object")
    for key, value in policy.items():
        valid = True
        if key == "tries":
            valid = type(value) is int and _number(value)
        elif key == "timeout":
            valid = _number(value)
        elif key == "backoff":
            valid = all(_number(v) for v in value) if isinstance(value, list) else _number(value)
        elif key == "fail_on_timeout":
            valid = type(value) is bool
        if not valid:
            raise MalformedEnvelopeError(f"Envelope policy {key} has an invalid value")
    known_policy = {"tries", "backoff", "timeout", "fail_on_timeout"}
    known_section = {
        "version",
        "job",
        "args",
        "kwargs",
        "policy",
        "queue",
        "dispatched_at",
        "context",
    }
    extra: dict[str, JSONValue] = {
        "top_level": {
            k: v for k, v in top.items() if k not in {"uuid", "displayName", ENVELOPE_KEY}
        },
        "section": {k: v for k, v in section.items() if k not in known_section},
        "policy": {k: v for k, v in policy.items() if k not in known_policy},
    }
    try:
        retry_policy = RetryPolicy(
            tries=cast(int | None, policy.get("tries")),
            backoff=cast(float | list[float] | None, policy.get("backoff")),
            timeout=cast(float | None, policy.get("timeout")),
            fail_on_timeout=cast(bool | None, policy.get("fail_on_timeout")),
        )
    except (ConfigurationError, ValueError, TypeError, OverflowError):
        raise MalformedEnvelopeError("Envelope policy is invalid") from None
    return Envelope(
        uuid=cast(str, top["uuid"]),
        display_name=cast(str, top["displayName"]),
        job=cast(str, section["job"]),
        args=tuple(args),
        kwargs=kwargs,
        policy=retry_policy,
        queue=cast(str | None, section.get("queue")),
        dispatched_at=cast(str | None, section.get("dispatched_at")),
        context=cast(dict[str, str], context),
        extra=extra,
    )


def peek_display_name(body: str) -> str:
    """Read displayName even from unsupported envelopes, within the same JSON limits."""
    try:
        name = _parse(body).get("displayName")
    except MalformedEnvelopeError:
        return ""
    return name if isinstance(name, str) else ""
