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

from collections.abc import Mapping
from dataclasses import dataclass, field

from ..codecs import JSONValue
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
    """Unknown keys (top level and section) preserved for forward compatibility."""


def encode_envelope(envelope: Envelope) -> str:
    """Compact JSON (``separators=(",", ":")``, ``ensure_ascii=False``, ``allow_nan=False``)."""
    raise NotImplementedError


def decode_envelope(body: str) -> Envelope:
    """Strict decode with the limits above. Raises JobDefectError subclasses only."""
    raise NotImplementedError


def peek_display_name(body: str) -> str:
    """Best-effort ``displayName`` for failure records of undecodable bodies; ``""`` if none."""
    raise NotImplementedError
