"""The parameter classification rules and the dependency walk edge cases."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Annotated, Any, Union

import pytest
from fastapi import Depends, FastAPI, Security
from starlette.requests import HTTPConnection, Request
from starlette.websockets import WebSocket

from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.fastapi import JobContext, LaravelCloudQueues
from laravel_cloud_queues.fastapi._depends import (
    SignatureCall,
    dependency_parameters,
    evaluated_parameters,
    inspecting,
    is_job_context,
    parameter_is_injected,
    reject_request_dependencies,
    request_kind,
)
from laravel_cloud_queues.fastapi._invoker import FastAPIInvoker
from laravel_cloud_queues.jobs.context import current_job


class _Registry:
    def __init__(self, invoker: FastAPIInvoker) -> None:
        self.invoker = invoker

    def job(self, func: Callable[..., Any] | None = None, /, **_kwargs: Any) -> Any:
        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn

        if func is not None:
            return decorate(func)
        return decorate


def _queues() -> LaravelCloudQueues:
    app = FastAPI()
    return LaravelCloudQueues(app, registry=_Registry(FastAPIInvoker(app)))  # type: ignore[arg-type]


def _parameter(annotation: object, default: object = inspect.Parameter.empty) -> inspect.Parameter:
    return inspect.Parameter(
        "value", inspect.Parameter.KEYWORD_ONLY, annotation=annotation, default=default
    )


class _NoSignature:
    """A callable ``inspect.signature`` cannot describe."""

    @property
    def __signature__(self) -> inspect.Signature:
        raise ValueError("no signature")

    def __call__(self) -> str:
        return "opaque"


class _Plain:
    pass


class _Service:
    def __init__(self, label: str = "svc") -> None:
        self.label = label


def test_request_kind_sees_through_unions_and_string_annotations() -> None:
    assert request_kind(Union[Request, None]) == "Request"  # noqa: UP007
    assert request_kind(WebSocket | None) == "WebSocket"
    assert request_kind(int | None) is None
    assert request_kind("Request") == "Request"
    assert request_kind("starlette.requests.HTTPConnection") == "HTTPConnection"
    assert request_kind("int") is None
    assert request_kind(HTTPConnection) == "HTTPConnection"
    assert request_kind(Annotated[Request, "meta"]) == "Request"
    # A parameterized generic is not a class; it is a payload annotation.
    assert request_kind(list[int]) is None
    assert request_kind(_Plain) is None


def test_is_job_context_accepts_annotated_strings_and_subclasses() -> None:
    class Sub(JobContext):
        pass

    assert is_job_context(Annotated[JobContext, "meta"]) is True
    assert is_job_context(Annotated[int, "meta"]) is False
    assert is_job_context("JobContext") is True
    assert is_job_context("laravel_cloud_queues.JobContext") is True
    assert is_job_context("Context") is False
    assert is_job_context(Sub) is True
    assert is_job_context(list[int]) is False


def test_string_annotations_are_classified_outside_registration() -> None:
    """Without ``inspecting()`` there is nothing to evaluate a postponed annotation against."""
    assert parameter_is_injected(_parameter("JobContext")) is True
    assert parameter_is_injected(_parameter("int")) is False


def test_inspecting_evaluates_only_the_handler_being_registered() -> None:
    def handler(job: JobContext, count: int) -> None:
        assert job and count

    with inspecting(handler):
        assert parameter_is_injected(_parameter("JobContext").replace(name="job")) is True
        assert parameter_is_injected(_parameter("JobContext").replace(name="other")) is True
        assert parameter_is_injected(_parameter("int").replace(name="count")) is False


def test_unresolvable_hints_fall_back_to_the_raw_annotations() -> None:
    queues = _queues()

    def job(request: Request, other: int) -> None:
        assert request and other

    # ``get_type_hints`` fails on the bogus name; the string annotations are inspected as-is.
    job.__annotations__ = {"request": "Request", "other": "NoSuchType"}
    with pytest.raises(ConfigurationError, match="Request parameter 'request'"):
        queues.job(job)


def test_annotated_metadata_without_depends_is_a_payload_parameter() -> None:
    queues = _queues()

    @queues.job
    def job(count: Annotated[int, "positive"], job: Annotated[JobContext, "delivery"]) -> None:
        assert count and job

    assert [p.name for p in dependency_parameters(job)] == []


def test_depends_without_a_callable_resolves_the_annotation() -> None:
    queues = _queues()

    @queues.job
    def job(
        annotated: Annotated[_Service, Depends()],
        default: _Service = Depends(),  # noqa: B008
    ) -> None:
        assert annotated and default

    assert [p.name for p in dependency_parameters(job)] == ["annotated", "default"]


def test_signature_call_refuses_to_run() -> None:
    call = SignatureCall(inspect.Signature())
    assert inspect.signature(call).parameters == {}
    with pytest.raises(RuntimeError, match="solved, not called"):
        call()


def test_dependencies_fastapi_cannot_analyze_are_configuration_errors() -> None:
    queues = _queues()

    def opaque(value: _Plain = _Plain()) -> _Plain:  # noqa: B008
        return value

    def job(value: _Plain = Depends(opaque)) -> None:  # noqa: B008
        assert value

    with pytest.raises(ConfigurationError, match="could not be analyzed"):
        queues.job(job)


def test_dependencies_without_a_signature_are_left_to_fastapi() -> None:
    """A callable ``inspect.signature`` rejects is skipped by the walk, at the top level
    and as an override; FastAPI's own analysis then reports it."""
    queues = _queues()
    opaque = _NoSignature()

    def job(value: str = Depends(opaque)) -> None:
        assert value

    with pytest.raises(ConfigurationError, match="could not be analyzed"):
        queues.job(job)

    def clean() -> str:
        return "ok"

    def overridden(value: str = Depends(clean)) -> None:
        assert value

    queues.job(overridden)
    queues.app.dependency_overrides[clean] = opaque
    queues.job(overridden)


def test_nested_security_scopes_are_configuration_errors() -> None:
    queues = _queues()

    def get_user() -> str:
        return "user"

    def scoped(user: str = Security(get_user, scopes=["read"])) -> str:
        return user

    def job(user: str = Depends(scoped)) -> None:
        assert user

    with pytest.raises(ConfigurationError, match="security scopes"):
        queues.job(job)


def test_overrides_are_walked_once_and_checked_for_queue_job_safety() -> None:
    queues = _queues()
    overrides = queues.app.dependency_overrides

    def clean() -> str:
        return "ok"

    def job(left: str = Depends(clean), right: str = Depends(clean)) -> None:
        assert left and right

    queues.job(job)

    def needs_name(name: str) -> str:
        return name

    overrides[clean] = needs_name
    with pytest.raises(ConfigurationError, match="parameter 'name'"):
        queues.job(job)

    def needs_request(request: Request) -> str:
        return request.url.path

    overrides[clean] = needs_request
    with pytest.raises(ConfigurationError, match=r"Request \(dependency '.*needs_request"):
        queues.job(job)

    def get_user() -> str:
        return "user"

    def needs_security(user: str = Security(get_user)) -> str:
        return user

    overrides[clean] = needs_security
    with pytest.raises(ConfigurationError, match=r"Security\(\) \(dependency"):
        queues.job(job)

    def nested(inner: str = Depends(needs_request)) -> str:
        return inner

    overrides[clean] = nested
    with pytest.raises(ConfigurationError, match="Request"):
        queues.job(job)

    def fine(label: str = "a", other: str = "b") -> str:
        return label + other

    overrides[clean] = fine
    queues.job(job)


def test_dependency_parameters_a_queue_job_cannot_supply() -> None:
    queues = _queues()

    class Builder:
        def build(self, size: int = 1) -> int:
            return size

    def with_varargs(*args: int, **kwargs: int) -> int:
        return len(args) + len(kwargs)

    def defaults_only(
        size: int = 1,
        label: str = "x",
        job: JobContext = Depends(current_job),  # noqa: B008
    ) -> str:
        return f"{label}{size}{job.job_name}"

    def job(
        unbound: int = Depends(Builder.build),
        star: int = Depends(with_varargs),
        text: str = Depends(defaults_only),
    ) -> None:
        assert unbound and star and text

    queues.job(job)

    def required(size: int) -> int:
        return size

    def broken(size: int = Depends(required)) -> None:
        assert size

    with pytest.raises(
        ConfigurationError, match=r"dependency 'test_dep.*required' parameter 'size'"
    ):
        queues.job(broken)


def test_reject_request_dependencies_accepts_an_omitted_override_mapping() -> None:
    def job(user_id: int) -> None:
        assert user_id

    reject_request_dependencies(job)


def test_evaluated_parameters_rejects_a_non_callable() -> None:
    with pytest.raises(TypeError, match="is not a callable object"):
        evaluated_parameters(42)
