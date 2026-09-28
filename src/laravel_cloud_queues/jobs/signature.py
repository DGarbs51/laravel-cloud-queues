"""Handler signature inspection and argument validation (§8 trust boundary).
CONTRACT — implemented by lane L3b."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..codecs import CodecRegistry, JSONValue

InjectedPredicate = Callable[[inspect.Parameter], bool]
"""True when a parameter is supplied at run time (JobContext, FastAPI ``Depends``), never
from payload data."""


@dataclass(frozen=True)
class JobSignature:
    func: Callable[..., Any]
    signature: inspect.Signature
    hints: Mapping[str, object]
    """Resolved annotations (``typing.get_type_hints(include_extras=True)``)."""
    serialized: tuple[str, ...]
    """Parameter names carried in the payload."""
    injected: tuple[str, ...]
    """Parameter names supplied at run time."""


def inspect_handler(func: Callable[..., Any], *, is_injected: InjectedPredicate) -> JobSignature:
    """Inspect once at registration. ``*args``/``**kwargs`` handler parameters are rejected
    with ConfigurationError (they cannot be validated)."""
    raise NotImplementedError


def encode_arguments(
    sig: JobSignature,
    codecs: CodecRegistry,
    args: Sequence[object],
    kwargs: Mapping[str, object],
) -> tuple[tuple[JSONValue, ...], dict[str, JSONValue]]:
    """Dispatch side: bind to the serialized parameters (injected parameters may not be
    passed -> ArgumentError), then encode (SerializationError). Positional/keyword shape is
    preserved as the caller used it."""
    raise NotImplementedError


def decode_arguments(
    sig: JobSignature,
    codecs: CodecRegistry,
    args: Sequence[JSONValue],
    kwargs: Mapping[str, JSONValue],
) -> tuple[list[object], dict[str, object]]:
    """Worker side: bind (ArgumentMismatchError: missing/extra/injected names supplied by the
    payload), then decode each value against its annotation (CodecError). Validation happens
    before execution; binding alone is not validation."""
    raise NotImplementedError
