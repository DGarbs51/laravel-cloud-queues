"""Handler signature inspection and argument validation."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, get_type_hints

from ..codecs import CodecRegistry, JSONValue, default_codecs
from ..errors import ArgumentError, ArgumentMismatchError, CodecError, ConfigurationError

InjectedPredicate = Callable[[inspect.Parameter], bool]
"""A predicate that determines if a parameter is injected at run time.

Injected parameters, such as a ``JobContext`` or a FastAPI ``Depends``, are never supplied
from payload data.
"""


@dataclass(frozen=True)
class JobSignature:
    """The inspected signature of a job handler."""

    func: Callable[..., Any]
    """The handler function."""
    signature: inspect.Signature
    """The handler's signature with its annotations resolved."""
    hints: Mapping[str, object]
    """The resolved annotations, from ``typing.get_type_hints(include_extras=True)``."""
    serialized: tuple[str, ...]
    """The names of the parameters carried in the payload."""
    injected: tuple[str, ...]
    """The names of the parameters injected at run time."""


def inspect_handler(
    func: Callable[..., Any],
    *,
    is_injected: InjectedPredicate,
    codecs: CodecRegistry | None = None,
) -> JobSignature:
    """Inspect the given handler when it is registered.

    Handlers declaring ``*args`` or ``**kwargs`` cannot be validated, so they raise a
    :class:`ConfigurationError`, as do unresolvable annotations. Annotations are checked
    against the given codec registry, or a fresh default registry when none is given.
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
    """Encode the given arguments for dispatch.

    The arguments are bound to the serialized parameters, and passing an injected parameter
    raises an :class:`ArgumentError`. Each value is encoded, raising a
    :class:`SerializationError` on failure, then checked with the worker's annotation
    decoder, where a mismatch raises an :class:`ArgumentError`. The caller's positional and
    keyword shape is preserved and defaults stay omitted.
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
    """Decode the given payload arguments for execution.

    The arguments are bound first, raising an :class:`ArgumentMismatchError` for missing,
    extra or injected names in the payload. Each value is then decoded against its
    annotation, raising a :class:`CodecError` on failure. Validation happens before
    execution, since binding alone is not validation.
    """
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
    """Bind the arguments to the serialized parameters of the handler.

    Raises the given error type if an injected parameter is supplied or the arguments do
    not bind.
    """
    if any(name in kwargs for name in sig.injected):
        raise error("Injected parameters cannot be supplied in job arguments")
    serialized = sig.signature.replace(
        parameters=[sig.signature.parameters[name] for name in sig.serialized]
    )
    try:
        return serialized.bind(*args, **kwargs)
    except TypeError:
        raise error("Arguments do not bind to the serialized job signature") from None
