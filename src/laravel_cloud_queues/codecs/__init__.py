"""Annotation-driven value codecs; payloads never name Python classes or imports.

Tags have exactly ``$type`` and ``value`` keys. Dictionaries containing ``$type``
are escaped with the ``dict`` tag; non-string keys are rejected. Union members
are tried in declaration order (so integral floats may select an earlier int).
Any/unannotated values allow JSON containers and known scalar tags, but never
construct dataclasses, models, custom classes or tagged tuples. Dataclasses and
models use plain objects and are constructed only from the trusted annotation.
"""

from __future__ import annotations

import base64
import binascii
import inspect
import math
import types
from dataclasses import MISSING, fields, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import (
    Annotated,
    Any,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)
from uuid import UUID

from ..errors import CodecError, ConfigurationError, SerializationError

try:
    from pydantic import BaseModel
except ImportError:  # Optional dependency; never imported based on payload content.
    BaseModel = None  # type: ignore[assignment,misc]

JSONValue: TypeAlias = bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"] | None
T = TypeVar("T")
TYPE_TAG_KEY = "$type"
_SCALARS = {
    "uuid": UUID,
    "datetime": datetime,
    "date": date,
    "time": time,
    "decimal": Decimal,
    "bytes": bytes,
}
_TAGS = {*_SCALARS, "tuple", "dict"}
_MAX_DEPTH = 64


class Codec(Protocol, Generic[T]):
    """Custom codec; the registry wraps its JSON value in a tag."""

    @property
    def tag(self) -> str: ...

    @property
    def python_type(self) -> type[T]: ...

    def encode(self, value: T) -> JSONValue: ...

    def decode(self, data: JSONValue) -> T: ...


def _model(annotation: object) -> bool:
    return (
        BaseModel is not None and isinstance(annotation, type) and issubclass(annotation, BaseModel)
    )


def _json(value: object, depth: int = 0) -> JSONValue:
    """Validate custom-codec output and direct decode input without trusting casts."""
    if depth > _MAX_DEPTH:
        raise ValueError("Value nesting limit exceeded")
    if value is None or type(value) in (bool, int, str):
        return cast(JSONValue, value)
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_json(v, depth + 1) for v in value]
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return {k: _json(v, depth + 1) for k, v in value.items()}
    raise ValueError("Expected a finite JSON value with string object keys")


class CodecRegistry:
    """Built-ins plus explicitly registered, registry-local custom codecs."""

    def __init__(self) -> None:
        # Heterogeneous codec types are erased only in this internal lookup.
        self._custom: dict[type[Any], Codec[Any]] = {}
        self._tags: dict[str, Codec[Any]] = {}

    def register(self, codec: Codec[T]) -> None:
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
        self._custom[codec.python_type] = codec
        self._tags[codec.tag] = codec

    def _validate_annotation(self, annotation: object, seen: tuple[object, ...] = ()) -> None:
        """Registration-time validation, including nested fields and recursive types."""
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
            self._validate_annotation(args[0], seen)
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
                    self._validate_annotation(arg, seen)
            return
        if origin is Literal and all(
            type(a) in (str, int, bool, type(None)) or isinstance(a, Enum) for a in args
        ):
            return
        if isinstance(annotation, type):
            if annotation in self._custom or issubclass(annotation, Enum):
                return
            if is_dataclass(annotation):
                hints = get_type_hints(annotation, include_extras=True)
                for field in fields(annotation):
                    if field.init:
                        self._validate_annotation(hints[field.name], seen)
                return
            if _model(annotation):
                model = cast("type[BaseModel]", annotation)
                for model_field in model.model_fields.values():
                    self._validate_annotation(model_field.annotation, seen)
                return
        raise ConfigurationError("Unsupported serialized parameter annotation")

    def encode(self, value: object) -> JSONValue:
        """Encode native values; invalid, cyclic or too-deep values are dispatch errors."""
        try:
            return self._encode(value, 0)
        except Exception:
            raise SerializationError("Argument cannot be serialized") from None

    def _encode(self, value: object, depth: int) -> JSONValue:
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
        if isinstance(value, tuple):
            return {TYPE_TAG_KEY: "tuple", "value": [self._encode(v, depth + 1) for v in value]}
        if is_dataclass(value) and not isinstance(value, type):
            value = {f.name: getattr(value, f.name) for f in fields(value) if f.init}
        elif BaseModel is not None and isinstance(value, BaseModel):
            value = {name: getattr(value, name) for name in type(value).model_fields}
        if isinstance(value, dict):
            if not all(isinstance(k, str) for k in value):
                raise ValueError("Dictionary keys must be strings")
            encoded = {k: self._encode(v, depth + 1) for k, v in value.items()}
            return {TYPE_TAG_KEY: "dict", "value": encoded} if TYPE_TAG_KEY in value else encoded
        if isinstance(value, list):
            return [self._encode(v, depth + 1) for v in value]
        return _json(value, depth)

    def decode(self, data: JSONValue, annotation: object) -> object:
        """Validate first; decoding failures are deterministic job defects."""
        try:
            return self._decode(_json(data), annotation)
        except Exception:
            raise CodecError("Value does not match its declared annotation") from None

    def _decode(self, data: JSONValue, annotation: object) -> object:
        origin, args = get_origin(annotation), get_args(annotation)
        if origin is Annotated:
            return self._decode(data, args[0])
        if origin in (Union, types.UnionType):
            for member in args:
                try:
                    return self._decode(data, member)
                except Exception:
                    pass
            raise ValueError("No union member matches")
        if isinstance(annotation, type) and issubclass(annotation, Enum):
            for member in annotation:
                try:
                    value = self._decode(data, type(member.value))
                    if type(value) is type(member.value) and value == member.value:
                        return member
                except Exception:
                    pass
            raise ValueError("Unknown enum value")
        if origin is Literal:
            for literal in args:
                if isinstance(literal, Enum):
                    try:
                        if self._decode(data, type(literal)) is literal:
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
            if set(data) != {TYPE_TAG_KEY, "value"} or not isinstance(data[TYPE_TAG_KEY], str):
                raise ValueError("Malformed tag")
            tag = cast(str, data[TYPE_TAG_KEY])
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
        elif annotation is float and type(data) in (int, float):
            number = float(cast(float, data))
            if math.isfinite(number):
                return number
        elif annotation is str and isinstance(data, str):
            return data
        elif (annotation is list or origin is list) and tag is None and isinstance(data, list):
            return [self._decode(v, args[0] if args else Any) for v in data]
        elif (
            (annotation is tuple or origin is tuple)
            and tag == "tuple"
            and isinstance(payload, list)
        ):
            if not args and annotation is tuple:
                return tuple(self._decode(v, Any) for v in payload)
            if len(args) == 2 and args[1] is Ellipsis:
                return tuple(self._decode(v, args[0]) for v in payload)
            if len(args) == len(payload):
                return tuple(self._decode(v, a) for v, a in zip(payload, args, strict=True))
        elif (annotation is dict or origin is dict) and tag in (None, "dict"):
            if isinstance(payload, dict) and (not args or args[0] is str):
                return {k: self._decode(v, args[1] if args else Any) for k, v in payload.items()}
        elif untyped and tag in (None, "dict"):
            if isinstance(payload, list):
                return [self._decode(v, Any) for v in payload]
            if isinstance(payload, dict):
                return {k: self._decode(v, Any) for k, v in payload.items()}
            return payload
        elif isinstance(annotation, type) and tag is None:
            if is_dataclass(annotation) and isinstance(data, dict):
                hints = get_type_hints(annotation, include_extras=True)
                declared = {f.name: f for f in fields(annotation) if f.init}
                if data.keys() - declared.keys() or any(
                    f.name not in data and f.default is MISSING and f.default_factory is MISSING
                    for f in declared.values()
                ):
                    raise ValueError("Dataclass fields mismatch")
                return annotation(**{k: self._decode(v, hints[k]) for k, v in data.items()})
            elif _model(annotation) and isinstance(data, dict):
                model = cast("type[BaseModel]", annotation)
                if data.keys() - model.model_fields.keys():
                    raise ValueError("Unknown model field")
                decoded = {
                    model.model_fields[k].alias or k: self._decode(
                        v, model.model_fields[k].annotation
                    )
                    for k, v in data.items()
                }
                return model.model_validate(decoded, strict=True)
        raise ValueError("Annotation mismatch")


def default_codecs() -> CodecRegistry:
    """Return an independent registry with built-in support."""
    return CodecRegistry()


__all__ = ["TYPE_TAG_KEY", "Codec", "CodecRegistry", "JSONValue", "default_codecs"]
