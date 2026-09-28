"""Value codecs (PROJECT_SCOPE.md §8). CONTRACT — implemented by lane L3b.

Encoding turns handler arguments into JSON values. Decoding is ANNOTATION-DRIVEN: the
registered handler's type hints choose the target type (dataclass, enum, Pydantic model,
UUID, datetime, custom codec type...). Payload data never names a Python type, module or
callable. Tagged objects use the reserved key ``"$type"`` (e.g. ``{"$type": "uuid",
"value": "..."}``); a user dict containing ``"$type"`` is escaped on encode, and on decode
any object with ``"$type"`` must be a known tag with exactly the expected shape, otherwise
CodecError.

Built-in support: JSON primitives, list, dict[str, T], tuple (tagged), dataclasses, Enum,
UUID, datetime/date/time (ISO 8601, tz preserved), Decimal, Optional/Union of supported
types, Literal, Any (primitives + tagged scalars only). Pydantic v2 ``BaseModel`` when
Pydantic is importable. Custom types via :meth:`CodecRegistry.register`.
"""

from __future__ import annotations

from typing import Generic, Protocol, TypeAlias, TypeVar

JSONValue: TypeAlias = bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"] | None

T = TypeVar("T")

TYPE_TAG_KEY = "$type"


class Codec(Protocol, Generic[T]):
    """Custom codec for one Python type. ``tag`` must be unique and not a built-in tag."""

    @property
    def tag(self) -> str: ...

    @property
    def python_type(self) -> type[T]: ...

    def encode(self, value: T) -> JSONValue: ...

    def decode(self, data: JSONValue) -> T: ...


class CodecRegistry:
    """Encodes values and decodes them against annotations."""

    def __init__(self) -> None:
        raise NotImplementedError

    def register(self, codec: Codec[T]) -> None:
        """Duplicate tag or type -> ConfigurationError."""
        raise NotImplementedError

    def encode(self, value: object) -> JSONValue:
        """Unsupported value -> SerializationError (dispatch-side)."""
        raise NotImplementedError

    def decode(self, data: JSONValue, annotation: object) -> object:
        """Decode and validate against ``annotation``. Mismatch -> CodecError (job defect)."""
        raise NotImplementedError


def default_codecs() -> CodecRegistry:
    """A new registry with the built-in codecs (and Pydantic when installed)."""
    raise NotImplementedError


__all__ = ["TYPE_TAG_KEY", "Codec", "CodecRegistry", "JSONValue", "default_codecs"]
