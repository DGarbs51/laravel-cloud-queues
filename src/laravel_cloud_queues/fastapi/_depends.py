"""Which job parameters FastAPI injects, and which HTTP-only dependencies are refused.

``Depends()`` defaults are real objects even when annotations are postponed. Postponed
``Annotated[..., Depends(...)]`` forms are resolved while :class:`LaravelCloudQueues` is
registering the handler (``inspecting``). ``JobContext`` annotations are injected either way.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin, get_type_hints

from fastapi import params
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import get_dependant
from fastapi.security import SecurityScopes
from starlette.background import BackgroundTasks
from starlette.requests import HTTPConnection, Request
from starlette.responses import Response
from starlette.websockets import WebSocket

from ..errors import ConfigurationError
from ..jobs.context import JobContext

QUEUE_JOB_PATH = "/__laravel_cloud_queues__/job"

_inspecting_handler: ContextVar[Callable[..., Any] | None] = ContextVar(
    "laravel_cloud_queues_fastapi_handler",
    default=None,
)

# More specific types before HTTPConnection, which Request and WebSocket subclass.
_REQUEST_TYPES: tuple[tuple[type[Any], str], ...] = (
    (WebSocket, "WebSocket"),
    (Request, "Request"),
    (HTTPConnection, "HTTPConnection"),
    (Response, "Response"),
    (BackgroundTasks, "BackgroundTasks"),
    (SecurityScopes, "SecurityScopes"),
)
_REQUEST_NAMES = {label for _, label in _REQUEST_TYPES}


@contextmanager
def inspecting(func: Callable[..., Any]) -> Iterator[None]:
    """Expose ``func`` while registration asks the invoker which parameters are injected."""

    token: Token[Callable[..., Any] | None] = _inspecting_handler.set(func)
    try:
        yield
    finally:
        _inspecting_handler.reset(token)


def parameter_is_injected(parameter: inspect.Parameter) -> bool:
    """True when the worker must supply this parameter (never the payload)."""

    parameter = _with_evaluated_annotation(parameter)
    return _injected(parameter)


def reject_request_dependencies(
    func: Callable[..., Any],
    overrides: Mapping[Callable[..., Any], Callable[..., Any]] | None = None,
) -> None:
    """Raise :class:`ConfigurationError` when a job declares HTTP-only dependencies.

    Serialized parameters are not part of the FastAPI walk, so ordinary payload annotations
    are left to the codec. Nested ``Depends()`` callables are walked with FastAPI's own
    ``get_dependant``. ``overrides`` is ``app.dependency_overrides`` (checked again at
    invoke, because tests can replace a dependency after registration).
    """

    parameters = evaluated_parameters(func)
    for parameter in parameters:
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            raise ConfigurationError(
                f"Job [{_qualname(func)}] cannot declare *args or **kwargs; "
                "every parameter must be validated."
            )
        kind = request_kind(parameter.annotation) or request_marker(parameter)
        if kind is not None:
            raise _http_error(func, f"{kind} parameter {parameter.name!r}")
        depends = depends_of(parameter)
        if isinstance(depends, params.Security):
            raise _http_error(func, f"Security() parameter {parameter.name!r}")

    injected = [parameter for parameter in parameters if depends_of(parameter) is not None]
    if not injected:
        return
    # Signature walk first: FastAPI's own analysis raises on Form()/File() without
    # python-multipart before the marker could be reported.
    for parameter in injected:
        _reject_markers(_dependency_call(parameter), func, overrides or {}, seen=set())
    try:
        dependant = get_dependant(
            path=QUEUE_JOB_PATH,
            call=_signature_call(inspect.Signature(injected)),
        )
    except Exception as exc:
        raise ConfigurationError(
            f"Job [{_qualname(func)}] dependencies could not be analyzed: {exc}"
        ) from exc
    _walk(dependant, func, overrides or {}, seen=set())


def dependency_parameters(func: Callable[..., Any]) -> tuple[inspect.Parameter, ...]:
    """Injected parameters FastAPI should solve. Plain ``JobContext`` is not included."""

    return tuple(
        parameter
        for parameter in evaluated_parameters(func)
        if _injected(parameter) and not _plain_job_context(parameter)
    )


def plain_job_context_parameters(func: Callable[..., Any]) -> tuple[inspect.Parameter, ...]:
    return tuple(
        parameter for parameter in evaluated_parameters(func) if _plain_job_context(parameter)
    )


def evaluated_parameters(func: Callable[..., Any]) -> tuple[inspect.Parameter, ...]:
    signature = inspect.signature(func)
    hints = _type_hints(func)
    parameters: list[inspect.Parameter] = []
    for name, parameter in signature.parameters.items():
        if name in hints:
            parameter = parameter.replace(annotation=hints[name])
        parameters.append(parameter)
    return tuple(parameters)


def depends_of(parameter: inspect.Parameter) -> params.Depends | None:
    if isinstance(parameter.default, params.Depends):
        return parameter.default
    return annotated_depends(parameter.annotation)


def request_marker(parameter: inspect.Parameter) -> str | None:
    """Name of a FastAPI request-parameter marker (``Header()``, ``Query()``, ``Cookie()``,
    ``Body()``, ``Path()``, ``Form()``, ``File()``) declared as the default or in
    ``Annotated`` metadata, else None. Queue jobs have no request to read them from."""

    candidates: list[object] = [parameter.default]
    if get_origin(parameter.annotation) is Annotated:
        candidates.extend(get_args(parameter.annotation)[1:])
    for candidate in candidates:
        if isinstance(candidate, (params.Param, params.Body)):
            return f"{type(candidate).__name__}()"
    return None


def annotated_depends(annotation: object) -> params.Depends | None:
    if get_origin(annotation) is not Annotated:
        return None
    found: params.Depends | None = None
    for arg in get_args(annotation)[1:]:
        if isinstance(arg, params.Depends):
            found = arg
    return found


def is_job_context(annotation: object) -> bool:
    if annotation is JobContext:
        return True
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        return bool(args) and is_job_context(args[0])
    if isinstance(annotation, str):
        return annotation == "JobContext" or annotation.endswith(".JobContext")
    if isinstance(annotation, type):
        try:
            return issubclass(annotation, JobContext)
        except TypeError:
            return False
    return False


def request_kind(annotation: object) -> str | None:
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        return request_kind(args[0]) if args else None
    if origin in (Union, UnionType):
        for arg in get_args(annotation):
            kind = request_kind(arg)
            if kind is not None:
                return kind
        return None
    if isinstance(annotation, str):
        name = annotation.rsplit(".", 1)[-1]
        if name in _REQUEST_NAMES:
            return name
        return None
    for cls, label in _REQUEST_TYPES:
        if annotation is cls:
            return label
        if isinstance(annotation, type):
            try:
                if issubclass(annotation, cls):
                    return label
            except TypeError:
                continue
    return None


def _injected(parameter: inspect.Parameter) -> bool:
    if request_kind(parameter.annotation) is not None:
        return True
    if isinstance(parameter.default, params.Depends):
        return True
    if annotated_depends(parameter.annotation) is not None:
        return True
    return is_job_context(parameter.annotation)


def _plain_job_context(parameter: inspect.Parameter) -> bool:
    return is_job_context(parameter.annotation) and depends_of(parameter) is None


def _type_hints(func: Callable[..., Any]) -> Mapping[str, Any]:
    try:
        return get_type_hints(func, include_extras=True)
    except Exception:
        return {}


def _with_evaluated_annotation(parameter: inspect.Parameter) -> inspect.Parameter:
    """Resolve postponed annotations of the handler currently being registered."""

    if not isinstance(parameter.annotation, str):
        return parameter
    func = _inspecting_handler.get()
    if func is None:
        return parameter
    for candidate in evaluated_parameters(func):
        if candidate.name == parameter.name:
            return candidate
    return parameter


def _signature_call(signature: inspect.Signature) -> Callable[..., None]:
    def dependency_call(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("queue job dependencies are solved, not called")

    dependency_call.__signature__ = signature  # ty: ignore[unresolved-attribute]
    return dependency_call


def _dependency_call(parameter: inspect.Parameter) -> Callable[..., Any] | None:
    """The callable ``Depends()`` resolves for ``parameter`` (the annotation when omitted)."""

    depends = depends_of(parameter)
    if depends is None:
        return None
    if depends.dependency is not None:
        return depends.dependency
    annotation = parameter.annotation
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    return annotation if callable(annotation) else None


def _reject_markers(
    call: Callable[..., Any] | None,
    func: Callable[..., Any],
    overrides: Mapping[Callable[..., Any], Callable[..., Any]],
    *,
    seen: set[int],
) -> None:
    """Refuse ``Header()``/``Query()``/``Cookie()``/``Body()``/``Path()``/``Form()``/``File()``
    anywhere in the dependency tree, following ``dependency_overrides``."""

    if call is None or id(call) in seen:
        return
    seen.add(id(call))
    call = overrides.get(call, call)
    try:
        parameters = evaluated_parameters(call)
    except (TypeError, ValueError):
        return
    for parameter in parameters:
        marker = request_marker(parameter)
        if marker is not None:
            raise _http_error(
                func,
                f"{marker} parameter {parameter.name!r} "
                f"(dependency {getattr(call, '__qualname__', call)!r})",
            )
        _reject_markers(_dependency_call(parameter), func, overrides, seen=seen)


def _walk(
    dependant: Dependant,
    func: Callable[..., Any],
    overrides: Mapping[Callable[..., Any], Callable[..., Any]],
    *,
    seen: set[int],
) -> None:
    kind = _dependant_request_kind(dependant)
    if kind is not None:
        name = dependant.name or getattr(dependant.call, "__qualname__", "dependency")
        raise _http_error(func, f"{kind} (dependency {name!r})")
    for sub in dependant.dependencies:
        call = sub.call
        if call is not None and call in overrides:
            _walk_override(overrides[call], func, overrides, seen=seen)
            continue
        if _scopes(sub):
            name = sub.name or getattr(sub.call, "__qualname__", "dependency")
            raise _http_error(func, f"security scopes (dependency {name!r})")
        missing = _unresolved_parameter(call)
        if missing is not None:
            call_name = getattr(call, "__qualname__", "dependency")
            raise ConfigurationError(
                f"Job [{_qualname(func)}] dependency {call_name!r} parameter {missing!r} "
                "has no value in a queue job. Give it a default or provide it with Depends()."
            )
        _walk(sub, func, overrides, seen=seen)


def _walk_override(
    call: Callable[..., Any],
    func: Callable[..., Any],
    overrides: Mapping[Callable[..., Any], Callable[..., Any]],
    *,
    seen: set[int],
) -> None:
    marker = id(call)
    if marker in seen:
        return
    seen.add(marker)
    missing = _unresolved_parameter(call)
    if missing is not None:
        call_name = getattr(call, "__qualname__", "dependency")
        raise ConfigurationError(
            f"Job [{_qualname(func)}] dependency {call_name!r} parameter {missing!r} "
            "has no value in a queue job. Give it a default or provide it with Depends()."
        )
    try:
        parameters = evaluated_parameters(call)
    except (TypeError, ValueError):
        parameters = ()
    for parameter in parameters:
        kind = request_kind(parameter.annotation) or request_marker(parameter)
        if kind is not None:
            raise _http_error(func, f"{kind} (dependency {getattr(call, '__qualname__', call)!r})")
        if isinstance(depends_of(parameter), params.Security):
            raise _http_error(
                func,
                f"Security() (dependency {getattr(call, '__qualname__', call)!r})",
            )
    try:
        dependant = get_dependant(path=QUEUE_JOB_PATH, call=call)
    except (TypeError, ValueError):
        return
    _walk(dependant, func, overrides, seen=seen)


def _dependant_request_kind(dependant: Dependant) -> str | None:
    checks = (
        ("request_param_name", "Request"),
        ("websocket_param_name", "WebSocket"),
        ("http_connection_param_name", "HTTPConnection"),
        ("response_param_name", "Response"),
        ("background_tasks_param_name", "BackgroundTasks"),
        ("security_scopes_param_name", "SecurityScopes"),
    )
    for attr, label in checks:
        if getattr(dependant, attr, None):
            return label
    return None


def _scopes(dependant: Dependant) -> list[object]:
    own = getattr(dependant, "own_oauth_scopes", None)
    if not own:
        own = getattr(dependant, "security_scopes", None)
    if isinstance(own, list):
        return [*own]
    return []


def _unresolved_parameter(call: Callable[..., Any] | None) -> str | None:
    if call is None:
        return None
    try:
        parameters = evaluated_parameters(call)
    except (TypeError, ValueError):
        return None
    for parameter in parameters:
        if parameter.name == "self":
            continue
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        # Plain JobContext is not filled by FastAPI. Dependencies must use
        # Depends(current_job) (or another Depends) to receive the delivery.
        if depends_of(parameter) is not None or request_kind(parameter.annotation) is not None:
            continue
        if parameter.default is inspect.Parameter.empty:
            return parameter.name
    return None


def _http_error(func: Callable[..., Any], detail: str) -> ConfigurationError:
    return ConfigurationError(
        f"Job [{_qualname(func)}] cannot use {detail} because a queue job has no HTTP "
        "request. Use Depends() for application services, JobContext for the delivery, "
        "and app.state for resources created in the application lifespan."
    )


def _qualname(func: Callable[..., Any]) -> str:
    return str(getattr(func, "__qualname__", "<job>"))
