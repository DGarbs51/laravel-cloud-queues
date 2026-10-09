# Optional/Union syntax is deliberately exercised for supported annotations.
# ruff: noqa: UP007, UP045
from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Annotated, Any, Literal, Optional, Union
from uuid import UUID

import pytest

from laravel_cloud_queues import codecs as codecs_module
from laravel_cloud_queues.codecs import default_codecs
from laravel_cloud_queues.errors import (
    CodecError,
    ConfigurationError,
    DispatchError,
    JobDefectError,
    SerializationError,
)


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Number(IntEnum):
    ONE = 1


@dataclass
class Child:
    colors: list[Color]
    when: datetime | None
    pair: tuple[int, str]


@dataclass
class Parent:
    child: Child
    label: str = "default"
    counts: list[int] = field(default_factory=list)
    computed: int = field(default=9, init=False)


@pytest.mark.parametrize(
    ("value", "annotation"),
    [
        (None, type(None)),
        (True, bool),
        (3, int),
        (1.5, float),
        ("café", str),
        ([1, 2], list[int]),
        ({"x": 4}, dict[str, int]),
        ((1, "a"), tuple[int, str]),
        ((1, 2), tuple[int, ...]),
        ((), tuple[()]),
        (UUID("17dfcfa3-76ce-4500-af61-03701b2b2d7f"), UUID),
        (datetime(2026, 9, 27, 12, 15, tzinfo=timezone(timedelta(hours=5, minutes=30))), datetime),
        (datetime(2026, 9, 27), datetime),
        (date(2026, 9, 27), date),
        (time(12, 15, tzinfo=UTC), time),
        (Decimal("123.4500"), Decimal),
        (b"\x00\xffhello", bytes),
        (Color.RED, Color),
        (Number.ONE, Number),
        (None, Optional[datetime]),
        (datetime(2026, 1, 1), Optional[datetime]),
        ("x", Union[int, str]),
        ("yes", Literal["yes", "no"]),
        (1, Annotated[int, "metadata"]),
        (Parent(Child([Color.RED, Color.BLUE], None, (3, "hi"))), Parent),
        ({"$type": "user data", "value": {"$type": "still data"}}, dict[str, Any]),
        ([{"value": [1, None, "x"]}], Any),
    ],
)
def test_roundtrip(value, annotation):
    codecs = default_codecs()
    result = codecs.decode(codecs.encode(value), annotation)
    assert result == value
    assert type(result) is type(value)


@pytest.mark.parametrize(
    ("data", "annotation"),
    [
        (True, int),
        (False, float),
        (1.1, int),
        (1, bool),
        ("3", int),
        (3, str),
        (None, int),
        ("invalid", Color),
        (True, Number),
        (True, Literal[1]),
        ([1], tuple[int]),
        ({"$type": "tuple", "value": [1]}, list[int]),
        ({"$type": "tuple", "value": [1]}, tuple[int, str]),
        ({"$type": "tuple", "value": [1]}, Any),
        ({"$type": "uuid", "value": "bad"}, UUID),
        ({"$type": "decimal", "value": "NaN"}, Decimal),
        ({"$type": "bytes", "value": "***"}, bytes),
        ({"$type": "datetime", "value": "2026-01-01T12:00:00"}, date),
        ({"$type": "uuid", "value": "17dfcfa3-76ce-4500-af61-03701b2b2d7f"}, str),
        ({"$type": "uuid"}, Any),
        ({"$type": "uuid", "value": "x", "extra": 1}, Any),
        ({"$type": [], "value": None}, Any),
        ({"$type": "unknown", "value": 3}, Any),
        ({"$type": "dict", "value": []}, dict[str, Any]),
        ({"$type": "dict", "value": {}}, Parent),
        ({"child": {}, "unknown": 1}, Parent),
        ({}, Parent),
        ({"colors": ["bad"], "when": None, "pair": {"$type": "tuple", "value": [1, "x"]}}, Child),
        (float("nan"), Any),
        (float("inf"), float),
        ({1: "bad"}, Any),
    ],
)
def test_invalid_values_are_job_defects(data, annotation):
    with pytest.raises(CodecError) as exc:
        default_codecs().decode(data, annotation)
    assert isinstance(exc.value, JobDefectError)
    assert not isinstance(exc.value, DispatchError)


@pytest.mark.parametrize(
    "value", [object(), {1: "bad"}, float("nan"), float("inf"), Decimal("Infinity")]
)
def test_unserializable_values_are_dispatch_errors(value):
    with pytest.raises(SerializationError) as exc:
        default_codecs().encode(value)
    assert isinstance(exc.value, DispatchError)
    assert not isinstance(exc.value, JobDefectError)


def test_numeric_and_union_rules():
    codecs = default_codecs()
    assert type(codecs.decode(2.0, int)) is int
    assert type(codecs.decode(2, float)) is float
    assert type(codecs.decode(2, Union[float, int])) is float
    assert type(codecs.decode(2.0, Union[int, float])) is int


def test_any_scalar_and_escaped_dict():
    codecs = default_codecs()
    value = {"$type": "not a type", "nested": [Decimal("1.2"), UUID(int=1)]}
    assert codecs.decode(codecs.encode(value), Any) == value
    assert codecs.decode({"__class__": "os.system"}, Any) == {"__class__": "os.system"}


def test_payload_cannot_import(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Payload triggered importlib")

    monkeypatch.setattr(importlib, "import_module", forbidden)
    codecs = default_codecs()
    for tag in ("os.system", "__class__", "subprocess.Popen"):
        with pytest.raises(CodecError):
            codecs.decode({"$type": tag, "value": "ignored"}, Any)
    assert codecs.decode({"__class__": "subprocess.Popen"}, dict[str, str]) == {
        "__class__": "subprocess.Popen"
    }


def test_cycles_and_direct_decode_depth_are_bounded():
    value = []
    value.append(value)
    with pytest.raises(SerializationError):
        default_codecs().encode(value)
    with pytest.raises(CodecError):
        default_codecs().decode(value, Any)


class Token:
    def __init__(self, text):
        self.text = text


class TokenCodec:
    tag = "token"
    python_type = Token

    def encode(self, value):
        return value.text

    def decode(self, data):
        if not isinstance(data, str):
            raise ValueError("bad token")
        return Token(data)


def test_custom_codec_is_explicit_and_registry_local():
    codecs = default_codecs()
    codecs.register(TokenCodec())
    encoded = codecs.encode(Token("abc"))
    assert encoded == {"$type": "token", "value": "abc"}
    assert codecs.decode(encoded, Token).text == "abc"
    for annotation in (Any, str):
        with pytest.raises(CodecError):
            codecs.decode(encoded, annotation)
    with pytest.raises(CodecError):
        default_codecs().decode(encoded, Token)
    with pytest.raises(ConfigurationError):
        codecs.register(TokenCodec())
    for tag, python_type in (("uuid", Token), ("fresh", int), ("", Token)):
        codec = TokenCodec()
        codec.tag, codec.python_type = tag, python_type
        with pytest.raises(ConfigurationError):
            default_codecs().register(codec)


def test_custom_output_must_be_json():
    class Broken(TokenCodec):
        def encode(self, value):
            return {1: "no"}

    codecs = default_codecs()
    codecs.register(Broken())
    with pytest.raises(SerializationError):
        codecs.encode(Token("x"))


def test_pydantic_roundtrip_and_strict_fields():
    pydantic = pytest.importorskip("pydantic")

    class Model(pydantic.BaseModel):
        child: Child
        when: datetime
        uid: UUID
        count: int = 1

    value = Model(
        child=Child([Color.RED], None, (1, "a")), when=datetime(2026, 1, 1), uid=UUID(int=1)
    )
    codecs = default_codecs()
    encoded = codecs.encode(value)
    assert codecs.decode(encoded, Model) == value
    for changed in (
        {**encoded, "count": "1"},
        {**encoded, "count": True},
        {**encoded, "unknown": 1},
        {"count": 1},
    ):
        with pytest.raises(CodecError):
            codecs.decode(changed, Model)


def test_enum_scalar_values_and_literals():
    class Tagged(Enum):
        PRICE = Decimal("1.23")
        PAIR = (1, "x")

    codecs = default_codecs()
    for member in Tagged:
        assert codecs.decode(codecs.encode(member), Tagged) is member
    assert codecs.decode("red", Literal[Color.RED]) is Color.RED
    assert codecs.decode("x", Literal[1, "x"]) == "x"
    with pytest.raises(CodecError):
        codecs.decode("blue", Literal[Color.RED])
    with pytest.raises(CodecError):
        codecs.decode(1.0, Literal[1])


@dataclass
class Circle:
    child: Shape | None
    kind: Literal["circle"] = "circle"


@dataclass
class Square:
    child: Shape | None
    kind: Literal["square"] = "square"


Shape = Circle | Square


@pytest.mark.timeout(2)
@pytest.mark.parametrize("valid", [False, True])
def test_recursive_union_work_is_bounded(valid):
    from time import perf_counter

    data = None if valid else 5
    for _ in range(60):
        data = {"child": data}
    codecs = default_codecs()
    started = perf_counter()
    if valid:
        assert isinstance(codecs.decode(data, Shape), Circle)
    else:
        with pytest.raises(CodecError):
            codecs.decode(data, Shape)
    assert perf_counter() - started < 0.5
    # Failure state must never leak into the next top-level call.
    assert codecs.decode({"child": None}, Shape) == Circle(None)


@pytest.mark.parametrize(
    "annotation",
    [
        Literal["yes", "no"],
        dict[str, list[Literal[1]]],
        Parent,
        Shape,
        list[Circle],
        tuple[int, ...],
    ],
)
def test_supported_annotations_validate(annotation):
    default_codecs().validate_annotation(annotation)


def test_pydantic_annotations_validate_field_by_field():
    pydantic = pytest.importorskip("pydantic")

    class Model(pydantic.BaseModel):
        child: Child

    class Unsupported(pydantic.BaseModel):
        anything: object

    default_codecs().validate_annotation(Model)
    with pytest.raises(ConfigurationError):
        default_codecs().validate_annotation(Unsupported)


def test_without_pydantic_models_are_plain_objects(monkeypatch):
    pydantic = pytest.importorskip("pydantic")

    class Model(pydantic.BaseModel):
        count: int = 1

    monkeypatch.setitem(sys.modules, "pydantic", None)
    assert codecs_module._pydantic_model_type() is None
    monkeypatch.setattr(codecs_module, "_BASE_MODEL", None)
    codecs = default_codecs()
    with pytest.raises(ConfigurationError):
        codecs.validate_annotation(Model)
    with pytest.raises(SerializationError):
        codecs.encode(Model())
    with pytest.raises(CodecError):
        codecs.decode({"count": 1}, Model)


def test_enum_literal_with_undecodable_value():
    with pytest.raises(CodecError):
        default_codecs().decode("purple", Literal[Color.RED])


def test_custom_codec_must_return_its_python_type():
    class Wrong(TokenCodec):
        def decode(self, data):
            return data

    codecs = default_codecs()
    codecs.register(Wrong())
    with pytest.raises(CodecError):
        codecs.decode({"$type": "token", "value": "abc"}, Token)


@pytest.mark.parametrize(
    ("data", "annotation"), [([1], dict[str, int]), ({"a": 1}, dict[int, int])]
)
def test_dictionary_annotation_mismatches(data, annotation):
    with pytest.raises(CodecError):
        default_codecs().decode(data, annotation)
