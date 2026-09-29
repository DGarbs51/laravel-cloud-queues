"""The annotation-driven codecs for job arguments.

Payloads never name Python classes or imports. Tags have exactly ``$type`` and
``value`` keys. Dictionaries containing ``$type`` are escaped with the ``dict`` tag,
and non-string keys are rejected. Union members are tried in declaration order, so an
integral float may select an earlier ``int`` member.

``Any`` and unannotated values allow JSON containers and known scalar tags, but never
construct dataclasses, models, custom classes or tagged tuples. Dataclasses and models
are encoded as plain objects and constructed only from the trusted annotation. Nested
tuples need explicit element annotations, since tuples inside ``Any`` are unsupported.
Finite ``Decimal`` exponents are not bounded beyond the JSON size and depth limits.
"""

from __future__ import annotations

import base64
import binascii
import inspect
import math
import types
from collections.abc import Callable
from dataclasses import MISSING, dataclass, fields, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeGuard,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from typing_extensions import TypeIs

from ..errors import CodecError, ConfigurationError, SerializationError

if TYPE_CHECKING:
    from pydantic import BaseModel


def _pydantic_model_type() -> type[BaseModel] | None:
    """Import the Pydantic model base class, if the optional dependency is installed.

    It is imported once here and never based on payload content.
    """
    try:
        from pydantic import BaseModel
    except ImportError:
        return None
    return BaseModel


_BASE_MODEL = _pydantic_model_type()
"""The Pydantic model base class, or ``None`` when Pydantic is not installed."""

JSONValue: TypeAlias = bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"] | None
"""A value that may be represented in JSON."""
T = TypeVar("T")
"""The Python type handled by a codec."""
TYPE_TAG_KEY = "$type"
"""The key that marks a tagged value."""
_SCALARS = {
    "uuid": UUID,
    "datetime": datetime,
    "date": date,
    "time": time,
    "decimal": Decimal,
    "bytes": bytes,
}
"""The built-in scalar tags and the Python types they represent."""
_TAGS = {*_SCALARS, "tuple", "dict"}
"""The tags reserved by the built-in codecs."""
_MAX_DEPTH = 64
"""The maximum nesting depth of an encoded or decoded value."""


class Codec(Protocol, Generic[T]):
    """A custom codec for a Python type.

    The registry wraps the codec's JSON value in a tag.
    """

    @property
    def tag(self) -> str:
        """Get the tag that identifies values encoded by the codec."""
        ...

    @property
    def python_type(self) -> type[T]:
        """Get the Python type handled by the codec."""
        ...

    def encode(self, value: T) -> JSONValue:
        """Encode the value as JSON."""
        ...

    def decode(self, data: JSONValue) -> T:
        """Decode the value from JSON."""
        ...


@dataclass(frozen=True)
class _Erased:
    """A registered custom codec with its Python type erased for the registry's lookups."""

    tag: str
    """The tag that identifies values encoded by the codec."""
    python_type: type[object]
    """The Python type handled by the codec."""
    encode: Callable[[object], JSONValue]
    """Encode a value of exactly ``python_type`` as JSON."""
    decode: Callable[[JSONValue], object]
    """Decode a value from JSON."""


def _erase(codec: Codec[T]) -> _Erased:
    """Erase the Python type of the codec, narrowing back to it at the encoding boundary."""

    def encode(value: object) -> JSONValue:
        """Encode the value, which the registry routes here only for an exact type match."""
        assert isinstance(value, codec.python_type)
        return codec.encode(value)

    return _Erased(codec.tag, codec.python_type, encode, codec.decode)


def _model(annotation: object) -> TypeGuard[type[BaseModel]]:
    """Determine if the annotation is a Pydantic model class."""
    return (
        _BASE_MODEL is not None
        and isinstance(annotation, type)
        and issubclass(annotation, _BASE_MODEL)
    )


def _is_class(annotation: object) -> TypeGuard[type[object]]:
    """Determine if the annotation is a class, narrowing it without unknown type arguments."""
    return isinstance(annotation, type)


def _is_list(value: object) -> TypeIs[list[object]]:
    """Determine if the value is a list, narrowing it without unknown type arguments."""
    return isinstance(value, list)


def _is_tuple(value: object) -> TypeIs[tuple[object, ...]]:
    """Determine if the value is a tuple, narrowing it without unknown type arguments."""
    return isinstance(value, tuple)


def _is_dict(value: object) -> TypeIs[dict[object, object]]:
    """Determine if the value is a dictionary, narrowing it without unknown type arguments."""
    return isinstance(value, dict)


def _string_keyed(value: dict[object, object]) -> dict[str, object]:
    """Ensure every key of the dictionary is a string."""
    entries: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("Dictionary keys must be strings")
        entries[key] = item
    return entries


def _json(value: object, depth: int = 0) -> JSONValue:
    """Ensure the value is finite JSON with string object keys, without trusting casts.

    This validates custom codec output and direct decode input. Raises a
    :class:`ValueError` if the value is invalid or nested too deeply.
    """
    if depth > _MAX_DEPTH:
        raise ValueError("Value nesting limit exceeded")
    if value is None or (isinstance(value, (bool, int, str)) and type(value) in (bool, int, str)):
        return value
    if isinstance(value, float) and type(value) is float and math.isfinite(value):
        return value
    if _is_list(value):
        return [_json(v, depth + 1) for v in value]
    if _is_dict(value):
        return {k: _json(v, depth + 1) for k, v in _string_keyed(value).items()}
    raise ValueError("Expected a finite JSON value with string object keys")


class CodecRegistry:
    """The registry of built-in and explicitly registered custom codecs.

    Custom codecs are local to the registry they are registered with.
    """

    def __init__(self) -> None:
        """Create a new codec registry instance."""
        self._custom: dict[type[object], _Erased] = {}
        self._tags: dict[str, _Erased] = {}

    def register(self, codec: Codec[T]) -> None:
        """Register a custom codec with the registry.

        Raises a :class:`ConfigurationError` if the tag or Python type is empty, reserved,
        already registered, or handled by a built-in codec.
        """
        if (
            not codec.tag
            or codec.tag in _TAGS
            or codec.tag in self._tags
            or codec.python_type in self._custom
            or codec.python_type
            in (*_SCALARS.values(), bool, int, float, str, list, tuple, dict, type(None))
            or issubclass(codec.python_type, Enum)
            or is_dataclass(codec.python_type)
            or _model(codec.python_type)
        ):
            raise ConfigurationError("Codec tag or Python type conflicts with an existing codec")
        erased = _erase(codec)
        self._custom[erased.python_type] = erased
        self._tags[erased.tag] = erased

    def validate_annotation(self, annotation: object, seen: tuple[object, ...] = ()) -> None:
        """Ensure the annotation is supported, including nested fields and recursive types.

        This runs when a handler is registered and raises a :class:`ConfigurationError` if
        any part of the annotation is unsupported.
        """
        if annotation in seen:
            return
        seen = (*seen, annotation)
        origin, args = get_origin(annotation), get_args(annotation)
        if annotation in (
            Any,
            inspect.Parameter.empty,
            None,
            type(None),
            bool,
            int,
            float,
            str,
            list,
            dict,
            tuple,
            *_SCALARS.values(),
        ):
            return
        if origin is Annotated:
            self.validate_annotation(args[0], seen)
            return
        if origin in (Union, types.UnionType, list, tuple, dict):
            if origin is dict:
                if args[0] is not str:
                    raise ConfigurationError(
                        "Only string-keyed dictionary annotations are supported"
                    )
                args = args[1:]
            for arg in args:
                if arg is not Ellipsis:
                    self.validate_annotation(arg, seen)
            return
        if origin is Literal and all(
            type(a) in (str, int, bool, type(None)) or isinstance(a, Enum) for a in args
        ):
            return
        if _is_class(annotation):
            if annotation in self._custom or issubclass(annotation, Enum):
                return
            if is_dataclass(annotation):
                hints = get_type_hints(annotation, include_extras=True)
                for field in fields(annotation):
                    if field.init:
                        self.validate_annotation(hints[field.name], seen)
                return
            if _model(annotation):
                for model_field in annotation.model_fields.values():
                    self.validate_annotation(model_field.annotation, seen)
                return
        raise ConfigurationError("Unsupported serialized parameter annotation")

    def encode(self, value: object) -> JSONValue:
        """Encode the native value as JSON.

        Raises a :class:`SerializationError` at dispatch time if the value is invalid,
        cyclic or nested too deeply.
        """
        try:
            return self._encode(value, 0)
        except Exception:
            raise SerializationError("Argument cannot be serialized") from None

    def _encode(self, value: object, depth: int) -> JSONValue:
        """Encode the value at the given nesting depth."""
        if depth > _MAX_DEPTH:
            raise ValueError("Value nesting limit exceeded")
        if isinstance(value, Enum):
            return self._encode(value.value, depth + 1)
        if type(value) in self._custom:
            codec = self._custom[type(value)]
            return {TYPE_TAG_KEY: codec.tag, "value": _json(codec.encode(value), depth + 1)}
        for tag, python_type in _SCALARS.items():
            if type(value) is python_type:
                if isinstance(value, bytes):
                    scalar = base64.b64encode(value).decode("ascii")
                elif isinstance(value, (datetime, date, time)):
                    scalar = value.isoformat()
                else:
                    if isinstance(value, Decimal) and not value.is_finite():
                        raise ValueError("Non-finite decimal")
                    scalar = str(value)
                return {TYPE_TAG_KEY: tag, "value": scalar}
        if _is_tuple(value):
            return {TYPE_TAG_KEY: "tuple", "value": [self._encode(v, depth + 1) for v in value]}
        entries: dict[object, object] | None = None
        if is_dataclass(value) and not isinstance(value, type):
            entries = {f.name: getattr(value, f.name) for f in fields(value) if f.init}
        elif _BASE_MODEL is not None and isinstance(value, _BASE_MODEL):
            entries = {name: getattr(value, name) for name in type(value).model_fields}
        elif _is_dict(value):
            entries = value
        if entries is not None:
            encoded: dict[str, JSONValue] = {
                k: self._encode(v, depth + 1) for k, v in _string_keyed(entries).items()
            }
            return {TYPE_TAG_KEY: "dict", "value": encoded} if TYPE_TAG_KEY in encoded else encoded
        if _is_list(value):
            return [self._encode(v, depth + 1) for v in value]
        return _json(value, depth)

    def decode(self, data: JSONValue, annotation: object) -> object:
        """Decode the JSON data into a value matching the annotation.

        The data is validated first. Raises a :class:`CodecError` on failure, since a
        value that does not match its annotation is a deterministic job defect.
        """
        try:
            return self._decode(_json(data), annotation, {})
        except Exception:
            raise CodecError("Value does not match its declared annotation") from None

    def _decode(
        self, data: JSONValue, annotation: object, failures: dict[tuple[int, int], object]
    ) -> object:
        """Decode the data against the annotation, remembering failed union members."""
        origin, args = get_origin(annotation), get_args(annotation)
        if origin is Annotated:
            return self._decode(data, args[0], failures)
        if origin in (Union, types.UnionType):
            for member in args:
                key = (id(data), id(member))
                if key in failures:
                    continue
                try:
                    return self._decode(data, member, failures)
                except Exception:
                    # Keep annotations alive: their ids must not be reused during this decode.
                    # Identity keys also support Annotated metadata that is not hashable.
                    failures[key] = member
            raise ValueError("No union member matches")
        cls = annotation if _is_class(annotation) else None
        if cls is not None and issubclass(cls, Enum):
            for member in cls:
                member_value: object = member.value
                try:
                    value = self._decode(data, type(member_value), failures)
                    if type(value) is type(member_value) and value == member_value:
                        return member
                except Exception:
                    pass
            raise ValueError("Unknown enum value")
        if origin is Literal:
            for literal in args:
                if isinstance(literal, Enum):
                    try:
                        if self._decode(data, type(literal), failures) is literal:
                            return literal
                    except Exception:
                        pass
                elif type(data) is type(literal) and data == literal:
                    return literal
            raise ValueError("Unknown literal value")
        untyped = annotation in (Any, inspect.Parameter.empty)
        tag: str | None = None
        payload = data
        if isinstance(data, dict) and TYPE_TAG_KEY in data:
            tagged = data[TYPE_TAG_KEY]
            if set(data) != {TYPE_TAG_KEY, "value"} or not isinstance(tagged, str):
                raise ValueError("Malformed tag")
            tag = tagged
            payload = data["value"]
            if tag not in _TAGS and tag not in self._tags:
                raise ValueError("Unknown tag")
            if tag in _SCALARS:
                target = _SCALARS[tag]
                if not (untyped or annotation is target) or not isinstance(payload, str):
                    raise ValueError("Scalar tag mismatch")
                if target is bytes:
                    try:
                        return base64.b64decode(payload, validate=True)
                    except binascii.Error:
                        raise ValueError("Invalid base64") from None
                if target in (date, datetime, time):
                    return target.fromisoformat(payload)
                if target is Decimal:
                    decimal = Decimal(payload)
                    if not decimal.is_finite():
                        raise ValueError("Non-finite decimal")
                    return decimal
                return UUID(payload)
            if tag in self._tags:
                codec = self._tags[tag]
                if annotation is not codec.python_type:
                    raise ValueError("Custom tag mismatch")
                result = codec.decode(payload)
                if not isinstance(result, codec.python_type):
                    raise ValueError("Custom codec returned the wrong type")
                return result
            if tag == "tuple" and annotation is not tuple and origin is not tuple:
                raise ValueError("Tuple tag mismatch")
            if tag == "dict" and (
                not isinstance(payload, dict)
                or not (untyped or annotation is dict or origin is dict)
            ):
                raise ValueError("Dictionary escape mismatch")
        if annotation in (None, type(None)) and data is None:
            return None
        if annotation is bool and type(data) is bool:
            return data
        elif annotation is int and type(data) in (int, float):
            if isinstance(data, int) or (isinstance(data, float) and data.is_integer()):
                return int(data)
        elif annotation is float and isinstance(data, (int, float)) and type(data) in (int, float):
            # The data is validated first, so the number is finite.
            return float(data)
        elif annotation is str and isinstance(data, str):
            return data
        elif (annotation is list or origin is list) and tag is None and isinstance(data, list):
            return [self._decode(v, args[0] if args else Any, failures) for v in data]
        elif (
            (annotation is tuple or origin is tuple)
            and tag == "tuple"
            and isinstance(payload, list)
        ):
            if not args and annotation is tuple:
                return tuple(self._decode(v, Any, failures) for v in payload)
            if len(args) == 2 and args[1] is Ellipsis:
                return tuple(self._decode(v, args[0], failures) for v in payload)
            if len(args) == len(payload):
                return tuple(
                    self._decode(v, a, failures) for v, a in zip(payload, args, strict=True)
                )
        elif (annotation is dict or origin is dict) and tag in (None, "dict"):
            if isinstance(payload, dict) and (not args or args[0] is str):
                return {
                    k: self._decode(v, args[1] if args else Any, failures)
                    for k, v in payload.items()
                }
        elif untyped and tag in (None, "dict"):
            if isinstance(payload, list):
                return [self._decode(v, Any, failures) for v in payload]
            if isinstance(payload, dict):
                return {k: self._decode(v, Any, failures) for k, v in payload.items()}
            return payload
        elif cls is not None and tag is None:
            if is_dataclass(cls) and isinstance(data, dict):
                hints = get_type_hints(cls, include_extras=True)
                declared = {f.name: f for f in fields(cls) if f.init}
                if data.keys() - declared.keys() or any(
                    f.name not in data and f.default is MISSING and f.default_factory is MISSING
                    for f in declared.values()
                ):
                    raise ValueError("Dataclass fields mismatch")
                return cls(**{k: self._decode(v, hints[k], failures) for k, v in data.items()})
            elif _model(cls) and isinstance(data, dict):
                model_fields = cls.model_fields
                if data.keys() - model_fields.keys():
                    raise ValueError("Unknown model field")
                decoded = {
                    model_fields[k].alias or k: self._decode(
                        v, model_fields[k].annotation, failures
                    )
                    for k, v in data.items()
                }
                return cls.model_validate(decoded, strict=True)
        raise ValueError("Annotation mismatch")


def default_codecs() -> CodecRegistry:
    """Create an independent registry with the built-in codecs."""
    return CodecRegistry()


__all__ = ["TYPE_TAG_KEY", "Codec", "CodecRegistry", "JSONValue", "default_codecs"]
