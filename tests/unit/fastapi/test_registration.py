"""Registration rejects HTTP-only dependencies and classifies injected parameters."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Annotated, Any

import pytest
from fastapi import Body, Cookie, Depends, FastAPI, File, Form, Header, Path, Query, Security
from fastapi.security import HTTPBearer
from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import Response
from starlette.websockets import WebSocket

from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.fastapi import JobContext, LaravelCloudQueues
from laravel_cloud_queues.fastapi._invoker import FastAPIInvoker
from laravel_cloud_queues.jobs.context import current_job


class _Registry:
    def __init__(self, invoker: FastAPIInvoker) -> None:
        self.invoker = invoker
        self.injected: list[str] = []
        self.kwargs: dict[str, Any] | None = None

    def job(self, func: Callable[..., Any] | None = None, /, **kwargs: Any) -> Any:
        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            self.kwargs = kwargs
            for name, parameter in inspect.signature(fn).parameters.items():
                if self.invoker.is_injected(parameter):
                    self.injected.append(name)
            return fn

        if func is not None:
            return decorate(func)
        return decorate


def _queues() -> tuple[LaravelCloudQueues, _Registry]:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    registry = _Registry(invoker)
    return LaravelCloudQueues(app, registry=registry), registry  # type: ignore[arg-type]


def get_mailer() -> str:
    return "smtp"


def test_postponed_annotations_mark_job_context_and_annotated_depends() -> None:
    queues, registry = _queues()

    @queues.job
    async def send_email(
        user_id: int,
        job: JobContext,
        mailer: Annotated[str, Depends(get_mailer)],
    ) -> None:
        assert user_id
        assert job
        assert mailer

    assert registry.injected == ["job", "mailer"]
    assert send_email.__name__ == "send_email"


def test_depends_default_and_current_job_are_injected() -> None:
    queues, registry = _queues()

    @queues.job(name="emails.send", queue="emails", tries=3)
    async def send_email(
        user_id: int,
        mailer: str = Depends(get_mailer),
        job: JobContext = Depends(current_job),  # noqa: B008
    ) -> None:
        assert user_id
        assert mailer
        assert job

    assert registry.injected == ["mailer", "job"]
    assert registry.kwargs == {
        "name": "emails.send",
        "queue": "emails",
        "tries": 3,
        "backoff": None,
        "timeout": None,
        "fail_on_timeout": None,
        "policy": None,
    }


def test_direct_call_does_not_resolve_depends() -> None:
    queues, _registry = _queues()
    called = False

    def get_db() -> str:
        nonlocal called
        called = True
        return "db"

    @queues.job
    async def send_email(user_id: int, db: str = Depends(get_db)) -> str:
        return f"{user_id}:{db}"

    assert awaitable(send_email(1, db="explicit")) == "1:explicit"
    assert called is False


def awaitable(value: Any) -> Any:
    import anyio

    async def _unwrap() -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

    return anyio.run(_unwrap, backend="asyncio")


@pytest.mark.parametrize(
    "builder",
    [
        lambda: _request_job,
        lambda: _websocket_job,
        lambda: _response_job,
        lambda: _background_job,
        lambda: _security_job,
        lambda: _bearer_job,
        lambda: _nested_request_job,
        lambda: _unresolved_job,
    ],
)
def test_http_only_dependencies_are_configuration_errors(
    builder: Callable[[], Callable[..., Any]],
) -> None:
    queues, _registry = _queues()
    func = builder()
    with pytest.raises(ConfigurationError, match="queue job"):
        queues.job(func)


def _request_job(request: Request) -> None:
    assert request


def _websocket_job(socket: WebSocket) -> None:
    assert socket


def _response_job(response: Response) -> None:
    assert response


def _background_job(tasks: BackgroundTasks) -> None:
    assert tasks


def _security_job(user: str = Security(get_mailer, scopes=["read"])) -> str:
    return user


_bearer = HTTPBearer()


def _bearer_job(creds: Any = Depends(_bearer)) -> None:  # noqa: B008
    assert creds


def _needs_request(request: Request) -> str:
    return request.url.path


def _nested_request_job(user: str = Depends(_needs_request)) -> str:
    return user


def _needs_name(name: str) -> str:
    return name


def _unresolved_job(name: str = Depends(_needs_name)) -> str:
    return name


_MARKERS: list[Callable[..., Any]] = [Header, Query, Cookie, Body, Path, Form, File]


def _marker_dependency(marker: Callable[..., Any]) -> Callable[..., Any]:
    def dependency(value: str = marker()) -> str:
        return value

    dependency.__qualname__ = f"{marker.__name__.lower()}_dependency"
    return dependency


@pytest.mark.parametrize("marker", _MARKERS, ids=lambda marker: marker.__name__)
def test_request_parameter_markers_in_dependencies_are_configuration_errors(
    marker: Callable[..., Any],
) -> None:
    """Header()/Query()/... read an HTTP request a queue job does not
    have. Reject at registration instead of failing (and retrying) every delivery."""
    queues, _registry = _queues()
    dependency = _marker_dependency(marker)

    def job(user_id: int, value: str = Depends(dependency)) -> None:
        assert value

    with pytest.raises(ConfigurationError, match=rf"{marker.__name__}\(\) parameter 'value'"):
        queues.job(job)


@pytest.mark.parametrize("marker", _MARKERS, ids=lambda marker: marker.__name__)
def test_request_parameter_markers_are_found_nested_annotated_and_on_the_handler(
    marker: Callable[..., Any],
) -> None:
    queues, _registry = _queues()
    leaf = _marker_dependency(marker)

    def middle(inner: str = Depends(leaf)) -> str:
        return inner

    def nested_job(value: str = Depends(middle)) -> None:
        assert value

    def annotated_dependency(value: str) -> str:
        return value

    # Postponed annotations in this module cannot name the loop variable; set the
    # evaluated ``Annotated`` form directly, as ``get_type_hints`` would return it.
    annotated_dependency.__annotations__ = {"value": Annotated[str, marker()], "return": str}

    def annotated_job(value: str = Depends(annotated_dependency)) -> None:
        assert value

    def handler_job(user_id: int, value: str = marker()) -> None:
        assert value

    for job in (nested_job, annotated_job, handler_job):
        with pytest.raises(ConfigurationError, match=rf"{marker.__name__}\(\)"):
            queues.job(job)


def test_request_parameter_marker_added_through_overrides_fails_at_invoke() -> None:
    queues, _registry = _queues()
    app = queues.app

    def clean() -> str:
        return "ok"

    def job(value: str = Depends(clean)) -> None:
        assert value

    queues.job(job)  # Registration passes: the override does not exist yet.
    app.dependency_overrides[clean] = _marker_dependency(Header)
    with pytest.raises(ConfigurationError, match=r"Header\(\) parameter 'value'"):
        queues.job(job)


def test_varargs_are_rejected() -> None:
    queues, _registry = _queues()

    def send_email(*args: int) -> None:
        assert args

    with pytest.raises(ConfigurationError, match="args"):
        queues.job(send_email)


def test_binds_given_registry_and_app_state() -> None:
    app = FastAPI()
    invoker = FastAPIInvoker(app)
    registry = _Registry(invoker)
    queues = LaravelCloudQueues(app, registry=registry)  # type: ignore[arg-type]
    assert queues.app is app
    assert queues.registry is registry
    assert app.state.laravel_cloud_queues is queues
