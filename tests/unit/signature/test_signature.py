from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

import pytest

from laravel_cloud_queues.codecs import default_codecs
from laravel_cloud_queues.errors import (
    ArgumentError,
    ArgumentMismatchError,
    CodecError,
    ConfigurationError,
    SerializationError,
)
from laravel_cloud_queues.jobs.signature import decode_arguments, encode_arguments, inspect_handler


class Injected:
    pass


def is_injected(parameter):
    return parameter.annotation is Injected


def handler(
    context: Injected, count: int, /, label: str = "default", *, when: datetime | None = None
):
    return count, label, when


def test_resolved_annotations_injection_and_shape():
    sig = inspect_handler(handler, is_injected=is_injected)
    assert sig.serialized == ("count", "label", "when")
    assert sig.injected == ("context",)
    assert sig.hints["context"] is Injected
    assert sig.signature.parameters["context"].annotation is Injected
    codecs = default_codecs()
    when = datetime(2026, 1, 1)
    args, kwargs = encode_arguments(sig, codecs, [3], {"when": when, "label": "hello"})
    assert args == (3,)
    assert list(kwargs) == ["when", "label"]
    decoded_args, decoded_kwargs = decode_arguments(sig, codecs, args, kwargs)
    assert decoded_args == [3]
    assert decoded_kwargs == {"when": when, "label": "hello"}
    assert handler(Injected(), *decoded_args, **decoded_kwargs) == (3, "hello", when)
    assert decode_arguments(sig, codecs, [3], {}) == ([3], {})  # Python applies defaults.


@pytest.mark.parametrize(
    ("args", "kwargs"),
    [
        ([], {}),
        ([1, "x", None], {}),
        ([1], {"context": "payload"}),
        ([], {"count": 1}),
        ([1], {"extra": 1}),
        ([1, "x"], {"label": "again"}),
    ],
)
def test_binding_errors_classified_by_side(args, kwargs):
    sig = inspect_handler(handler, is_injected=is_injected)
    codecs = default_codecs()
    with pytest.raises(ArgumentError):
        encode_arguments(sig, codecs, args, kwargs)
    with pytest.raises(ArgumentMismatchError):
        decode_arguments(sig, codecs, args, kwargs)


def test_encoding_validation_and_serialization_are_distinct():
    sig = inspect_handler(handler, is_injected=is_injected)
    codecs = default_codecs()
    for value in ("x", True, 1.5):
        with pytest.raises(ArgumentError):
            encode_arguments(sig, codecs, [value], {})
        with pytest.raises(CodecError):
            decode_arguments(sig, codecs, [value], {})
    with pytest.raises(SerializationError):
        encode_arguments(sig, codecs, [object()], {})


def test_kwargs_and_positional_order_are_preserved():
    def job(a: int, b: str, *, c: bool):
        pass

    sig = inspect_handler(job, is_injected=is_injected)
    codecs = default_codecs()
    args, kwargs = encode_arguments(sig, codecs, [], {"c": True, "b": "s", "a": 1})
    assert args == ()
    assert list(kwargs) == ["c", "b", "a"]
    assert decode_arguments(sig, codecs, args, kwargs) == ([], kwargs)


@pytest.mark.parametrize("func", [lambda *args: None, lambda **kwargs: None])
def test_variadic_handlers_rejected(func):
    with pytest.raises(ConfigurationError):
        inspect_handler(func, is_injected=is_injected)


@pytest.mark.parametrize(
    "annotation", [set[int], dict[int, str], Callable[[], int], object, list[object]]
)
def test_unsupported_annotations_fail_at_registration(annotation):
    def job(value):
        pass

    job.__annotations__["value"] = annotation
    with pytest.raises(ConfigurationError):
        inspect_handler(job, is_injected=is_injected)


def test_unresolvable_annotations_fail_at_registration():
    def job(value: UndefinedClass):  # noqa: F821
        pass

    with pytest.raises(ConfigurationError):
        inspect_handler(job, is_injected=is_injected)


def test_annotated_metadata_and_unannotated_values():
    def job(value: Annotated[int, "metadata"], other):
        pass

    sig = inspect_handler(job, is_injected=is_injected)
    assert sig.hints["value"] == Annotated[int, "metadata"]
    assert sig.signature.parameters["other"].annotation is inspect.Parameter.empty
    codecs = default_codecs()
    assert encode_arguments(sig, codecs, [1, {"values": [True, None]}], {}) == (
        (1, {"values": [True, None]}),
        {},
    )


@dataclass
class InvalidNested:
    unsupported: set[int]


def test_nested_unsupported_dataclass_rejected():
    def job(value: InvalidNested):
        pass

    with pytest.raises(ConfigurationError):
        inspect_handler(job, is_injected=is_injected)


def test_injected_unsupported_type_is_exempt():
    def job(value: object):
        pass

    sig = inspect_handler(job, is_injected=lambda parameter: True)
    assert sig.serialized == ()
    assert encode_arguments(sig, default_codecs(), [], {}) == ((), {})


def test_custom_codec_handler_registration_and_roundtrip():
    class Token:
        def __init__(self, text):
            self.text = text

    class TokenCodec:
        tag = "token"
        python_type = Token

        @staticmethod
        def encode(value):
            return value.text

        @staticmethod
        def decode(data):
            if not isinstance(data, str):
                raise ValueError("Expected token text")
            return Token(data)

    def job(value):
        return value.text

    # Supply the local class directly; get_type_hints cannot resolve local string names.
    job.__annotations__["value"] = Token
    codecs = default_codecs()
    codecs.register(TokenCodec())
    sig = inspect_handler(job, is_injected=is_injected, codecs=codecs)
    assert sig.serialized == ("value",)
    args, kwargs = encode_arguments(sig, codecs, [Token("hello")], {})
    decoded_args, decoded_kwargs = decode_arguments(sig, codecs, args, kwargs)
    assert job(*decoded_args, **decoded_kwargs) == "hello"

    # Custom registrations do not leak into either default or independent registries.
    for other_codecs in (None, default_codecs()):
        with pytest.raises(ConfigurationError):
            inspect_handler(job, is_injected=is_injected, codecs=other_codecs)
