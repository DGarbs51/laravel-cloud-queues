"""Handler signature inspection and argument validation."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, get_type_hints

from ..codecs import CodecRegistry, JSONValue, default_codecs
from ..errors import ArgumentError, ArgumentMismatchError, CodecError, ConfigurationError

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


def inspect_handler(
    func: Callable[..., Any],
    *,
    is_injected: InjectedPredicate,
    codecs: CodecRegistry | None = None,
) -> JobSignature:
    """Inspect once at registration. ``*args``/``**kwargs`` handler parameters are rejected
    with ConfigurationError (they cannot be validated). Annotations use the supplied
    codec registry, or a fresh default registry when ``codecs`` is None.
    """
    try:
        signature = inspect.signature(func)
        hints = get_type_hints(func, include_extras=True)
        parameters = tuple(
            p.replace(annotation=hints.get(p.name, p.annotation))
            for p in signature.parameters.values()
        )
        serialized: list[str] = []
        injected: list[str] = []
        if codecs is None:
            codecs = default_codecs()
        for parameter in parameters:
            if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                raise ConfigurationError("Job handlers cannot declare *args or **kwargs")
            if is_injected(parameter):
                injected.append(parameter.name)
            else:
                codecs._validate_annotation(parameter.annotation)
                serialized.append(parameter.name)
        return JobSignature(
            func,
            signature.replace(parameters=parameters),
            hints,
            tuple(serialized),
            tuple(injected),
        )
    except ConfigurationError:
        raise
    except Exception:
        raise ConfigurationError("Cannot resolve handler signature or annotations") from None


def encode_arguments(
    sig: JobSignature,
    codecs: CodecRegistry,
    args: Sequence[object],
    kwargs: Mapping[str, object],
) -> tuple[tuple[JSONValue, ...], dict[str, JSONValue]]:
    """Dispatch side: bind to the serialized parameters (injected parameters may not be
    passed -> ArgumentError), then encode (SerializationError). Positional/keyword shape is
    preserved as the caller used it. Encoded values are checked with the same annotation
    decoder as the worker; a mismatch becomes ArgumentError. Defaults stay omitted.
    """
    bound = _bind(sig, args, kwargs, ArgumentError)
    encoded: dict[str, JSONValue] = {}
    for name, value in bound.arguments.items():
        encoded[name] = codecs.encode(value)
        # Reuse worker validation to reject annotation mismatches before sending.
        # This validates the wire value without substituting it for the caller's value.
        try:
            codecs.decode(encoded[name], sig.hints.get(name, inspect.Parameter.empty))
        except CodecError:
            raise ArgumentError(f"Argument {name} does not match its annotation") from None
    names = tuple(bound.arguments)
    return (
        tuple(encoded[name] for name in names[: len(args)]),
        {name: encoded[name] for name in kwargs},
    )


def decode_arguments(
    sig: JobSignature,
    codecs: CodecRegistry,
    args: Sequence[JSONValue],
    kwargs: Mapping[str, JSONValue],
) -> tuple[list[object], dict[str, object]]:
    """Worker side: bind (ArgumentMismatchError: missing/extra/injected names supplied by the
    payload), then decode each value against its annotation (CodecError). Validation happens
    before execution; binding alone is not validation."""
    bound = _bind(sig, args, kwargs, ArgumentMismatchError)
    decoded = {
        name: codecs.decode(value, sig.hints.get(name, inspect.Parameter.empty))
        for name, value in bound.arguments.items()
    }
    names = tuple(bound.arguments)
    return (
        [decoded[name] for name in names[: len(args)]],
        {name: decoded[name] for name in kwargs},
    )


def _bind(
    sig: JobSignature,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    error: type[ArgumentError | ArgumentMismatchError],
) -> inspect.BoundArguments:
    if any(name in kwargs for name in sig.injected):
        raise error("Injected parameters cannot be supplied in job arguments")
    serialized = sig.signature.replace(
        parameters=[sig.signature.parameters[name] for name in sig.serialized]
    )
    try:
        return serialized.bind(*args, **kwargs)
    except TypeError:
        raise error("Arguments do not bind to the serialized job signature") from None
