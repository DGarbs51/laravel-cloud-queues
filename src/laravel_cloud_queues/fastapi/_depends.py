"""The rules for which job parameters FastAPI injects and which dependencies are refused.

``Depends()`` defaults are real objects even when annotations are postponed. Postponed
``Annotated[..., Depends(...)]`` forms are resolved while :class:`LaravelCloudQueues` is
registering the handler (``inspecting``). ``JobContext`` annotations are injected either way.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from types import UnionType
from typing import (
    Annotated,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from fastapi import params
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import get_dependant
from fastapi.security import SecurityScopes
from starlette.background import BackgroundTasks
from starlette.requests import HTTPConnection, Request
from starlette.responses import Response
from starlette.websockets import WebSocket
from typing_extensions import ParamSpec

from ..errors import ConfigurationError
from ..jobs.context import JobContext

QUEUE_JOB_PATH = "/__laravel_cloud_queues__/job"
"""The synthetic request path used when solving queue job dependencies."""

P = ParamSpec("P")
R = TypeVar("R")


@runtime_checkable
class AnyCallable(Protocol):
    """A callable whose signature is only known at run time, such as a dependency.

    ``isinstance`` checks for a ``__call__`` attribute, like :func:`callable`.
    """

    def __call__(self, *args: object, **kwargs: object) -> object: ...


Overrides = Mapping[AnyCallable, AnyCallable]
"""The shape of ``app.dependency_overrides``."""

_inspecting_parameters: ContextVar[tuple[inspect.Parameter, ...] | None] = ContextVar(
    "laravel_cloud_queues_fastapi_handler",
    default=None,
)
"""The evaluated parameters of the handler currently being registered, if any."""

# More specific types before HTTPConnection, which Request and WebSocket subclass.
_REQUEST_TYPES: tuple[tuple[type[object], str], ...] = (
    (WebSocket, "WebSocket"),
    (Request, "Request"),
    (HTTPConnection, "HTTPConnection"),
    (Response, "Response"),
    (BackgroundTasks, "BackgroundTasks"),
    (SecurityScopes, "SecurityScopes"),
)
"""The HTTP-only types a queue job cannot receive, with their display names."""
_REQUEST_NAMES = {label for _, label in _REQUEST_TYPES}
"""The display names of the HTTP-only types."""


@contextmanager
def inspecting(func: Callable[P, R]) -> Generator[None, None, None]:
    """Expose the handler while registration asks which of its parameters are injected."""

    token: Token[tuple[inspect.Parameter, ...] | None] = _inspecting_parameters.set(
        evaluated_parameters(func)
    )
    try:
        yield
    finally:
        _inspecting_parameters.reset(token)


def parameter_is_injected(parameter: inspect.Parameter) -> bool:
    """Determine if the worker must supply the parameter, rather than the payload."""

    parameter = _with_evaluated_annotation(parameter)
    return _injected(parameter)


def reject_request_dependencies(
    func: Callable[P, R],
    overrides: Overrides | None = None,
) -> None:
    """Ensure the job declares no HTTP-only dependencies.

    Raises a :class:`ConfigurationError` when it does, or when it declares ``*args`` or
    ``**kwargs``. Serialized parameters are not part of the FastAPI walk, so ordinary payload
    annotations are left to the codec. Nested ``Depends()`` callables are walked with FastAPI's
    own ``get_dependant``. The ``overrides`` are ``app.dependency_overrides``, checked again at
    invoke because tests can replace a dependency after registration.
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
            call=SignatureCall(inspect.Signature(injected)),
        )
    except Exception as exc:
        raise ConfigurationError(
            f"Job [{_qualname(func)}] dependencies could not be analyzed: {exc}"
        ) from exc
    _walk(dependant, func, overrides or {}, seen=set())


def dependency_parameters(func: Callable[P, R]) -> tuple[inspect.Parameter, ...]:
    """Get the injected parameters FastAPI should solve, excluding plain ``JobContext``."""

    return tuple(
        parameter
        for parameter in evaluated_parameters(func)
        if _injected(parameter) and not _plain_job_context(parameter)
    )


def evaluated_parameters(func: Callable[P, R]) -> tuple[inspect.Parameter, ...]:
    """Get the handler parameters with their postponed annotations evaluated."""
    signature = inspect.signature(func)
    hints = _type_hints(func)
    parameters: list[inspect.Parameter] = []
    for name, parameter in signature.parameters.items():
        if name in hints:
            parameter = parameter.replace(annotation=hints[name])
        parameters.append(parameter)
    return tuple(parameters)


def depends_of(parameter: inspect.Parameter) -> params.Depends | None:
    """Get the ``Depends()`` declared by the parameter, if any."""
    if isinstance(parameter.default, params.Depends):
        return parameter.default
    return annotated_depends(parameter.annotation)


def request_marker(parameter: inspect.Parameter) -> str | None:
    """Get the name of the request-parameter marker declared by the parameter, if any.

    The markers are ``Header()``, ``Query()``, ``Cookie()``, ``Body()``, ``Path()``, ``Form()``
    and ``File()``, as the default or in ``Annotated`` metadata. Queue jobs have no request to
    read them from.
    """

    candidates: list[object] = [parameter.default]
    if get_origin(parameter.annotation) is Annotated:
        candidates.extend(get_args(parameter.annotation)[1:])
    for candidate in candidates:
        if isinstance(candidate, (params.Param, params.Body)):
            return f"{type(candidate).__name__}()"
    return None


def annotated_depends(annotation: object) -> params.Depends | None:
    """Get the last ``Depends()`` in the ``Annotated`` metadata, if any."""
    if get_origin(annotation) is not Annotated:
        return None
    found: params.Depends | None = None
    for arg in get_args(annotation)[1:]:
        if isinstance(arg, params.Depends):
            found = arg
    return found


def is_job_context(annotation: object) -> bool:
    """Determine if the annotation refers to ``JobContext``."""
    if annotation is JobContext:
        return True
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        return bool(args) and is_job_context(args[0])
    if isinstance(annotation, str):
        return annotation == "JobContext" or annotation.endswith(".JobContext")
    return isinstance(annotation, type) and issubclass(annotation, JobContext)


def request_kind(annotation: object) -> str | None:
    """Get the name of the HTTP-only type referenced by the annotation, if any."""
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
    """Determine if the parameter is injected rather than taken from the payload."""
    if request_kind(parameter.annotation) is not None:
        return True
    if isinstance(parameter.default, params.Depends):
        return True
    if annotated_depends(parameter.annotation) is not None:
        return True
    return is_job_context(parameter.annotation)


def _plain_job_context(parameter: inspect.Parameter) -> bool:
    """Determine if the parameter is a ``JobContext`` without a ``Depends()``."""
    return is_job_context(parameter.annotation) and depends_of(parameter) is None


def _type_hints(func: Callable[P, R]) -> Mapping[str, object]:
    """Get the handler type hints, or an empty mapping when they cannot be evaluated."""
    try:
        return get_type_hints(func, include_extras=True)
    except Exception:
        return {}


def _with_evaluated_annotation(parameter: inspect.Parameter) -> inspect.Parameter:
    """Resolve the postponed annotation of the handler being registered."""

    if not isinstance(parameter.annotation, str):
        return parameter
    for candidate in _inspecting_parameters.get() or ():
        if candidate.name == parameter.name:
            return candidate
    return parameter


class SignatureCall:
    """A stand-in callable carrying a signature for FastAPI to analyze.

    ``inspect.signature`` reads ``__signature__``, so FastAPI sees exactly the injected
    parameters and never the handler's payload parameters.
    """

    def __init__(self, signature: inspect.Signature) -> None:
        """Create a new stand-in for the given signature."""
        self.__signature__ = signature

    def __call__(self, *_args: object, **_kwargs: object) -> None:
        """Refuse to be called, since queue job dependencies are only solved."""
        raise RuntimeError("queue job dependencies are solved, not called")


def _dependency_call(parameter: inspect.Parameter) -> AnyCallable | None:
    """Get the callable ``Depends()`` resolves, or the annotation when it is omitted."""

    depends = depends_of(parameter)
    if depends is None:
        return None
    call: object = depends.dependency
    if call is None:
        call = parameter.annotation
        if get_origin(call) is Annotated:
            call = get_args(call)[0]
    return call if isinstance(call, AnyCallable) else None


def _reject_markers(
    call: AnyCallable | None,
    func: Callable[P, R],
    overrides: Overrides,
    *,
    seen: set[int],
) -> None:
    """Ensure no request-parameter marker appears anywhere in the dependency tree.

    The markers are ``Header()``, ``Query()``, ``Cookie()``, ``Body()``, ``Path()``, ``Form()``
    and ``File()``. The walk follows ``dependency_overrides``.
    """

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
    func: Callable[P, R],
    overrides: Overrides,
    *,
    seen: set[int],
) -> None:
    """Ensure the dependency tree has no HTTP-only dependencies or unresolved parameters."""
    kind = _dependant_request_kind(dependant)
    if kind is not None:
        name = dependant.name or getattr(dependant.call, "__qualname__", "dependency")
        raise _http_error(func, f"{kind} (dependency {name!r})")
    for sub in dependant.dependencies:
        call = sub.call
        assert call is not None, "get_dependant always sets the sub-dependant call"
        if call in overrides:
            _walk_override(overrides[call], func, overrides, seen=seen)
            continue
        if _has_scopes(sub):
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
    call: AnyCallable,
    func: Callable[P, R],
    overrides: Overrides,
    *,
    seen: set[int],
) -> None:
    """Ensure an overriding dependency has no HTTP-only dependencies or unresolved parameters."""
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
    """Get the name of the HTTP-only type the dependant asks for, if any."""
    checks = (
        (dependant.request_param_name, "Request"),
        (dependant.websocket_param_name, "WebSocket"),
        (dependant.http_connection_param_name, "HTTPConnection"),
        (dependant.response_param_name, "Response"),
        (dependant.background_tasks_param_name, "BackgroundTasks"),
        (dependant.security_scopes_param_name, "SecurityScopes"),
    )
    for name, label in checks:
        if name:
            return label
    return None


def _has_scopes(dependant: Dependant) -> bool:
    """Determine if the dependant declares security scopes.

    FastAPI 0.123 renamed ``security_scopes`` to ``own_oauth_scopes``.
    """
    return bool(
        getattr(dependant, "own_oauth_scopes", None) or getattr(dependant, "security_scopes", None)
    )


def _unresolved_parameter(call: AnyCallable) -> str | None:
    """Get the name of the first dependency parameter a queue job cannot supply."""
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


def _http_error(func: object, detail: str) -> ConfigurationError:
    """Create the error raised when a job uses an HTTP-only dependency."""
    return ConfigurationError(
        f"Job [{_qualname(func)}] cannot use {detail} because a queue job has no HTTP "
        "request. Use Depends() for application services, JobContext for the delivery, "
        "and app.state for resources created in the application lifespan."
    )


def _qualname(func: object) -> str:
    """Get the qualified name of the given handler."""
    return str(getattr(func, "__qualname__", "<job>"))
