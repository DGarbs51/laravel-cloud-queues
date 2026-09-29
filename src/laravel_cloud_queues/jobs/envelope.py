"""The version 1 envelope that carries a job over the wire.

The envelope is one JSON object. Keys other than ``uuid`` and ``displayName`` live under the
versioned ``laravel_cloud_queues`` section::

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

The ``policy`` object omits any field the job did not declare. The ``retry_until`` and
``max_exceptions`` keys are reserved and are not emitted in version 1. Unknown keys inside
the section and at the top level are preserved on decode (as ``extra``) and otherwise
ignored. An envelope re-queued by a dashboard retry decodes identically, since attempts
always come from the transport and never from the body.

Decoding is bounded by ``MAX_BODY_BYTES`` in total and ``MAX_DEPTH`` levels of nesting. A
body over those limits, one containing NaN, Infinity or duplicate keys, or one whose top
level is not an object raises a :class:`MalformedEnvelopeError`. A Laravel overflow body
(``{"@pointer": ...}``) raises an :class:`UnsupportedOverflowPayloadError`, and any
``version`` other than 1 raises an :class:`UnsupportedEnvelopeVersionError`.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import NoReturn, cast

from ..codecs import JSONValue
from ..errors import (
    MalformedEnvelopeError,
    SerializationError,
    UnsupportedEnvelopeVersionError,
    UnsupportedOverflowPayloadError,
)
from .policy import MAX_JOB_TIMEOUT, RetryPolicy

ENVELOPE_KEY = "laravel_cloud_queues"
"""The key of the versioned section within the envelope."""
ENVELOPE_VERSION = 1
"""The envelope version written by this package."""
MAX_BODY_BYTES = 16 * 1_048_576
"""The maximum size of an envelope body in UTF-8 bytes."""
MAX_DEPTH = 64
"""The maximum nesting depth of an envelope body."""


@dataclass(frozen=True)
class Envelope:
    """The decoded contents of a job message."""

    uuid: str
    """The unique identifier of the logical dispatch."""
    display_name: str
    """The wire name of the job, recorded as the failed job's name."""
    job: str
    """The name used to look the job up in the registry."""
    args: tuple[JSONValue, ...] = ()
    """The encoded positional arguments."""
    kwargs: Mapping[str, JSONValue] = field(default_factory=dict[str, JSONValue])
    """The encoded keyword arguments."""
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    """The retry policy declared by the job."""
    queue: str | None = None
    """The logical queue the job was dispatched to, kept for debugging only."""
    dispatched_at: str | None = None
    """The ISO 8601 time at which the job was dispatched."""
    context: Mapping[str, str] = field(default_factory=dict[str, str])
    """The trace context propagated from the dispatcher."""
    extra: Mapping[str, JSONValue] = field(default_factory=dict[str, JSONValue])
    """The unknown keys preserved from the body.

    They are grouped into ``top_level``, ``section`` and ``policy`` object maps. Unknown or
    reserved policy fields are retained but have no runtime effect.
    """


def encode_envelope(envelope: Envelope) -> str:
    """Encode the envelope as deterministic, compact JSON.

    Undeclared policy fields and missing optional metadata are omitted. Raises a
    :class:`SerializationError` if the envelope cannot be serialized.
    """
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
            context={**envelope.context},
        )
        for key, value in (("queue", envelope.queue), ("dispatched_at", envelope.dispatched_at)):
            if value is not None:
                section[key] = value
            else:
                section.pop(key, None)
        top.update(uuid=envelope.uuid, displayName=envelope.display_name)
        top[ENVELOPE_KEY] = section
        body = json.dumps(
            top, separators=(",", ":"), ensure_ascii=False, allow_nan=False, sort_keys=True
        )
        body.encode("utf-8")
        return body
    except (ValueError, TypeError, RecursionError):
        raise SerializationError("Envelope cannot be serialized") from None


def _extras(envelope: Envelope, key: str) -> dict[str, JSONValue]:
    """Get a copy of the given namespace of the envelope's extras."""
    value = envelope.extra.get(key, {})
    if not isinstance(value, dict):
        raise ValueError("Envelope extras must be namespaced objects")
    return value.copy()


def _reject_constant(value: str) -> NoReturn:
    """Reject a NaN or Infinity constant in the JSON body."""
    raise ValueError("Non-finite JSON number")


def _float(value: str) -> float:
    """Parse a JSON number as a float, rejecting values that are not finite."""
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Non-finite JSON number")
    return result


def _pairs(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    """Build a JSON object from its pairs, rejecting duplicate keys."""
    result: dict[str, JSONValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _parse(body: str) -> dict[str, JSONValue]:
    """Parse the body into a JSON object within the decoding limits.

    Raises a :class:`MalformedEnvelopeError` if the body is invalid or exceeds a limit.
    """
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
    """Determine if the value is a non-negative number."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def decode_envelope(body: str) -> Envelope:
    """Decode the given body into an envelope.

    Decoding is strict and bounded. Unknown and reserved fields are preserved but ignored.
    """
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
            valid = _number(value) and cast(float, value) <= MAX_JOB_TIMEOUT
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
    except Exception:
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
    """Get the display name from the body, even when the envelope is unsupported.

    The same JSON limits apply. An empty string is returned when no name can be read.
    """
    try:
        name = _parse(body).get("displayName")
    except MalformedEnvelopeError:
        return ""
    return name if isinstance(name, str) else ""
