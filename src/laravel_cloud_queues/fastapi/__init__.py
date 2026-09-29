"""The FastAPI integration for queued jobs.

Requires FastAPI >= 0.121 (the ``[fastapi]`` extra). Importing this package is the only
place ``laravel_cloud_queues`` imports FastAPI.

:class:`LaravelCloudQueues` binds a :class:`~laravel_cloud_queues.registry.Registry` to a
FastAPI app (``app.state.laravel_cloud_queues``). Jobs may use ``Depends()`` and
``JobContext``. Each delivery gets a fresh dependency scope: ``app.dependency_overrides``
is honored, and sub-dependencies are cached only within that delivery. ``yield`` teardown
runs inside the invoker, before the worker acknowledges the message.

Direct calls do not run dependency injection. ``Job.__call__`` calls the raw function, so
injected parameters must be passed explicitly (a ``Depends()`` default is not resolved)::

    await send_email(1, mailer=mailer)

``Depends(current_job)`` and a parameter annotated ``JobContext`` both receive the delivery
context. Sync handlers run on the calling thread (the worker main thread). Sync
dependencies run in FastAPI's threadpool; their teardown order is preserved.

``yield`` dependencies observe the handler's exception (and the ``JobControl`` raised by
``JobContext.release()`` / ``fail()``) exactly as they observe a route's exception, so
``except`` blocks such as a transaction rollback run on failure and release. Teardown
after an explicit release or fail runs under a 10-second deadline that bounds **async**
teardown; sync ``yield`` teardown runs in FastAPI's threadpool, cannot be cancelled, and
is bounded only by the job timeout (SIGALRM).

:meth:`LaravelCloudQueues.lifespan` enters ``app.router.lifespan_context`` once per enter.
The worker enters it once per process and exits it on clean shutdown. Jobs can read
resources from ``app.state``, including identifier keys yielded by the lifespan (copied
onto ``app.state`` because a queue job has no ``Request``). ``on_startup`` / ``on_shutdown``
run when FastAPI's default lifespan is in use (no ``lifespan=`` argument). A custom lifespan
replaces them, matching FastAPI.

HTTP-only dependencies (``Request``, ``WebSocket``, ``HTTPConnection``, ``Response``,
``BackgroundTasks``, ``SecurityScopes``, ``Security()``) raise
:class:`~laravel_cloud_queues.errors.ConfigurationError` at registration when they can be
seen, and again at invoke for overrides added later.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import TypeVar, overload

from typing_extensions import ParamSpec

from ..config import QueueConfig
from ..jobs.context import JobContext, current_job
from ..jobs.job import Job
from ..jobs.policy import RetryPolicy
from ..registry import Registry

try:
    from fastapi import FastAPI
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "FastAPI support requires the optional dependency: "
        'pip install "laravel-cloud-queues[fastapi]"'
    ) from exc

from ._depends import inspecting, reject_request_dependencies
from ._invoker import FastAPIInvoker
from ._lifespan import enter_lifespan

P = ParamSpec("P")
R = TypeVar("R")


class LaravelCloudQueues:
    """The binding between a job registry and a FastAPI app.

    The instance is stored on ``app.state.laravel_cloud_queues``. Jobs may use
    ``Depends()`` (fresh scope per job, ``yield`` teardown before the outcome is
    acknowledged, ``app.dependency_overrides`` honored, sub-dependency cache per job) and
    ``JobContext`` via ``Depends(current_job)`` or a ``JobContext`` annotation. Request-only
    dependencies raise :class:`~laravel_cloud_queues.errors.ConfigurationError`.
    :meth:`lifespan` enters the app's lifespan once per worker process.
    """

    def __init__(
        self,
        app: FastAPI,
        *,
        registry: Registry | None = None,
        config: QueueConfig | None = None,
    ) -> None:
        """Create a new FastAPI queue binding instance.

        A registry using a :class:`FastAPIInvoker` is created when none is given.
        """
        self._app = app
        if registry is None:
            registry = Registry(config=config, invoker=FastAPIInvoker(app))
        self._registry = registry
        app.state.laravel_cloud_queues = self

    @property
    def app(self) -> FastAPI:
        """Get the FastAPI application."""
        return self._app

    @property
    def registry(self) -> Registry:
        """Get the job registry."""
        return self._registry

    @overload
    def job(self, func: Callable[P, R], /) -> Job[P, R]: ...

    @overload
    def job(
        self,
        func: None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        tries: int | None = None,
        backoff: float | Sequence[float] | None = None,
        timeout: float | None = None,
        fail_on_timeout: bool | None = None,
        policy: RetryPolicy | None = None,
    ) -> Callable[[Callable[P, R]], Job[P, R]]: ...

    def job(
        self,
        func: Callable[P, R] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        tries: int | None = None,
        backoff: float | Sequence[float] | None = None,
        timeout: float | None = None,
        fail_on_timeout: bool | None = None,
        policy: RetryPolicy | None = None,
    ) -> Job[P, R] | Callable[[Callable[P, R]], Job[P, R]]:
        """Register a handler as a queued job, either directly or as a decorator.

        Raises a :class:`~laravel_cloud_queues.errors.ConfigurationError` if the handler
        depends on a request-only dependency.
        """

        def register(fn: Callable[P, R]) -> Job[P, R]:
            """Register the given handler with the registry."""
            with inspecting(fn):
                reject_request_dependencies(fn, self._app.dependency_overrides)
                # Overloads only allow keywords when the function is omitted; the
                # returned decorator then receives the handler.
                decorate = self._registry.job(
                    name=name,
                    queue=queue,
                    tries=tries,
                    backoff=backoff,
                    timeout=timeout,
                    fail_on_timeout=fail_on_timeout,
                    policy=policy,
                )
                return decorate(fn)

        if func is not None:
            return register(func)
        return register

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        """Get a context manager that runs the app's lifespan around the worker."""
        return enter_lifespan(self._app)


__all__ = ["JobContext", "LaravelCloudQueues", "current_job"]
