"""The invoker that runs each job in its own FastAPI dependency scope.

FastAPI 0.121+ reads yield-dependency exit stacks from the request scope
(``fastapi_inner_astack`` and ``fastapi_function_astack``) and ignores the
``async_exit_stack`` argument. This invoker builds a synthetic request marked as a
queue job so those internals have a scope, then refuses user dependencies that ask
for ``Request`` or other HTTP-only types.

Sync dependencies run in FastAPI's threadpool (``run_in_threadpool``), including the
teardown of sync ``yield`` dependencies. Exit-stack order is still last-in, first-out.
Sync **handlers** are called on the invoking thread — the worker's main thread — so
``SIGALRM`` can interrupt them. They are not handed to a threadpool.

``yield`` teardown finishes before :meth:`FastAPIInvoker.invoke` returns, which is
before the worker acknowledges the delivery. As in a FastAPI request, the handler's
exception (or the :class:`JobControl` raised by an explicit release or fail) is thrown
into ``yield`` dependencies, so a ``try: yield db; db.commit() except: db.rollback()``
dependency rolls back on failure and release and commits only on success. After
:class:`JobControl`, teardown is bounded by :data:`TEARDOWN_DEADLINE_SECONDS`; the
deadline bounds async teardown only — sync ``yield`` teardown runs in FastAPI's
threadpool and is bounded only by the job timeout. A teardown error after a release or
fail is logged and the recorded outcome still propagates. A teardown error after a
successful handler propagates and becomes the handler failure.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from types import TracebackType
from typing import Literal

import anyio
from fastapi import FastAPI
from fastapi import __version__ as _fastapi_version
from fastapi.dependencies.utils import get_dependant, solve_dependencies
from starlette.requests import Request

from .._narrowing import is_mapping
from ..errors import ConfigurationError
from ..jobs.context import JobContext, JobControl, current_job
from ..jobs.job import AnyJob
from ..registry import bind_injected
from ._depends import (
    QUEUE_JOB_PATH,
    SignatureCall,
    dependency_parameters,
    parameter_is_injected,
    reject_request_dependencies,
)

_MIN_FASTAPI = (0, 121)
"""The minimum supported FastAPI version."""


def _version_tuple(version: str) -> tuple[int, int]:
    """Parse the major and minor numbers from a version string."""
    major_text, _, rest = version.partition(".")
    minor_digits: list[str] = []
    for char in rest:
        if not char.isdigit():
            break
        minor_digits.append(char)
    return int(major_text), int("".join(minor_digits) or "0")


if _version_tuple(_fastapi_version) < _MIN_FASTAPI:
    raise ImportError(
        "laravel_cloud_queues.fastapi requires FastAPI >= 0.121 "
        f"(found {_fastapi_version}). The dependency solver reads yield-dependency "
        "exit stacks from the request scope starting in that release."
    )

logger = logging.getLogger("laravel_cloud_queues.fastapi")
"""The logger for the FastAPI integration."""

TEARDOWN_DEADLINE_SECONDS = 10.0
"""The number of seconds async teardown may take after an explicit release or fail."""

_Outcome = Literal["pending", "success", "control", "error"]
"""The outcome of a handler, as recorded for dependency teardown."""


class _OverrideProvider:
    """The app's dependency overrides, with ``current_job`` bound to the delivery context."""

    def __init__(self, app: FastAPI, context: JobContext) -> None:
        """Create a new override provider instance."""
        overrides = dict(app.dependency_overrides)
        if current_job not in overrides:

            def provide_current_job() -> JobContext:
                """Get the context of the current delivery."""
                return context

            overrides[current_job] = provide_current_job
        self.dependency_overrides = overrides
        """The dependency overrides consulted by the FastAPI solver."""


class FastAPIInvoker:
    """A :class:`~laravel_cloud_queues.registry.Invoker` backed by FastAPI's dependency solver."""

    def __init__(self, app: FastAPI) -> None:
        """Create a new FastAPI invoker instance."""
        self._app = app

    def is_injected(self, parameter: inspect.Parameter) -> bool:
        """Determine if the given parameter is injected rather than taken from the payload."""
        return parameter_is_injected(parameter)

    async def invoke(
        self,
        job: AnyJob,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        context: JobContext,
    ) -> None:
        """Invoke the job's handler within a fresh dependency scope.

        ``yield`` teardown finishes before this method returns. The handler's exception,
        including a :class:`JobControl`, is re-raised after teardown.
        """
        func = job.func
        reject_request_dependencies(func, self._app.dependency_overrides)
        if not dependency_parameters(func):
            await _run_handler(job, args, kwargs, context, {})
            return

        request_stack = AsyncExitStack()
        function_stack = AsyncExitStack()
        await request_stack.__aenter__()
        await function_stack.__aenter__()
        outcome: _Outcome = "pending"
        caught: BaseException | None = None
        try:
            solved = await _solve(self._app, func, context, request_stack, function_stack)
            await _run_handler(job, args, kwargs, context, solved)
        except JobControl as exc:
            outcome = "control"
            caught = exc
        except Exception as exc:
            outcome = "error"
            caught = exc
        else:
            outcome = "success"
        finally:
            await _close_dependencies(
                function_stack,
                request_stack,
                outcome=outcome,
                context=context,
                exc=caught,
            )
        if caught is not None:
            raise caught


async def _solve(
    app: FastAPI,
    func: object,
    context: JobContext,
    request_stack: AsyncExitStack,
    function_stack: AsyncExitStack,
) -> dict[str, object]:
    """Resolve the handler's dependencies against a synthetic queue job request.

    Raises a :class:`ConfigurationError` if the dependencies cannot be resolved.
    """
    parameters = dependency_parameters(func)
    scope: dict[str, object] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": QUEUE_JOB_PATH,
        "raw_path": QUEUE_JOB_PATH.encode("ascii"),
        "query_string": b"",
        "headers": [],
        "client": None,
        "server": None,
        "root_path": "",
        "state": {},
        "app": app,
        "extensions": {"laravel_cloud_queues.job": True},
        "fastapi_inner_astack": request_stack,
        "fastapi_function_astack": function_stack,
    }
    provider = _OverrideProvider(app, context)
    # No ``dependency_cache`` is passed, so FastAPI builds a fresh one per delivery and
    # reuses a cached sub-dependency within this call only.
    solved = await solve_dependencies(
        request=Request(scope),
        dependant=get_dependant(
            path=QUEUE_JOB_PATH,
            call=SignatureCall(inspect.Signature(parameters)),
        ),
        dependency_overrides_provider=provider,
        async_exit_stack=request_stack,
        embed_body_fields=False,
    )
    if solved.errors:
        raise ConfigurationError(
            f"Job [{_qualname(func)}] dependencies could not be resolved: "
            f"{_format_errors(solved.errors)}"
        )
    return dict(solved.values)


async def _run_handler(
    job: AnyJob,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    context: JobContext,
    solved: Mapping[str, object],
) -> None:
    """Run the handler, awaiting its result when it is awaitable."""
    result = _call(job, args, kwargs, context, solved)
    if inspect.isawaitable(result):
        await result


def _call(
    job: AnyJob,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    context: JobContext,
    solved: Mapping[str, object],
) -> object:
    """Call the handler through :func:`bind_injected` and the inspected signature.

    Omitted defaults stay omitted, matching dispatch. FastAPI solves every injected
    parameter except a plain ``JobContext``, which receives this delivery's context.
    """

    signature = job.signature
    injected = {name: solved.get(name, context) for name in signature.injected}
    return job.call_bound(bind_injected(signature.signature, args, kwargs, injected))


async def _close_dependencies(
    function_stack: AsyncExitStack,
    request_stack: AsyncExitStack,
    *,
    outcome: _Outcome,
    context: JobContext,
    exc: BaseException | None,
) -> None:
    """Exit the per-job stacks the way FastAPI exits a request's.

    The handler's exception, or its ``JobControl``, is thrown into ``yield`` dependencies
    so their ``except`` and ``finally`` blocks see it, exactly as in a request. Re-raising
    it is normal teardown, which ``contextlib`` reports as "not suppressed" rather than
    raising, and suppressing it does not change the job outcome, since the caller still
    raises it. Any other exception is a teardown error: it propagates after a success and
    is logged otherwise.
    """
    try:
        if outcome == "control":
            with anyio.fail_after(TEARDOWN_DEADLINE_SECONDS):
                await _aexit(function_stack, request_stack, exc)
        else:
            await _aexit(function_stack, request_stack, exc)
    except Exception:
        if outcome == "success":
            raise
        logger.exception(
            "Dependency teardown failed after %s for job [%s]; the recorded outcome stands.",
            _outcome_phrase(outcome),
            context.job_name,
        )


async def _aexit(
    function_stack: AsyncExitStack,
    request_stack: AsyncExitStack,
    exc: BaseException | None,
) -> None:
    """Unwind the stacks as FastAPI's nested ``async with`` blocks would.

    The inner stack sees ``exc`` first. Whatever it raises, ``exc`` itself or a new
    exception, is what the outer stack sees, and when it suppresses, the outer stack exits
    cleanly. Returns normally when the exception was suppressed or when nothing was
    raised, leaving the caller to decide the outcome.
    """
    details = _exc_details(exc)
    try:
        if await function_stack.__aexit__(*details):
            details = (None, None, None)
    except BaseException as inner:
        if await request_stack.__aexit__(type(inner), inner, inner.__traceback__):
            return
        raise
    await request_stack.__aexit__(*details)


def _exc_details(
    exc: BaseException | None,
) -> tuple[type[BaseException] | None, BaseException | None, TracebackType | None]:
    """Get the exception details tuple expected by ``__aexit__``."""
    if exc is None:
        return (None, None, None)
    return (type(exc), exc, exc.__traceback__)


def _outcome_phrase(outcome: _Outcome) -> str:
    """Get the log phrase that describes the given outcome."""
    if outcome == "control":
        return "an explicit release or fail"
    if outcome == "pending":
        return "cancellation"
    return "a handler error"


def _format_errors(errors: Sequence[object]) -> str:
    """Format the dependency validation errors as a single message.

    FastAPI reports each error as a pydantic ``ErrorDetails`` mapping.
    """
    parts: list[str] = []
    for error in errors:
        if is_mapping(error):
            parts.append(f"{error.get('loc', ())}: {error.get('msg', 'invalid')}")
        else:
            parts.append(repr(error))
    return "; ".join(parts) or "dependency validation failed"


def _qualname(func: object) -> str:
    """Get the qualified name of the given handler."""
    return str(getattr(func, "__qualname__", "<job>"))
