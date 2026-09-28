"""FastAPI integration (PROJECT_SCOPE.md §13, §17). CONTRACT — implemented by lane L8.

Requires the ``[fastapi]`` extra. Importing this module is the only place the package
imports FastAPI.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, TypeVar, overload

from typing_extensions import ParamSpec

from ..config import QueueConfig
from ..jobs.context import JobContext, current_job
from ..jobs.job import Job
from ..jobs.policy import RetryPolicy
from ..registry import Registry

if TYPE_CHECKING:
    from fastapi import FastAPI

P = ParamSpec("P")
R = TypeVar("R")


class LaravelCloudQueues:
    """Binds a registry to a FastAPI app (``app.state.laravel_cloud_queues = self``).

    Jobs may use ``Depends()`` (fresh scope per job, ``yield`` teardown before the outcome is
    acknowledged, ``app.dependency_overrides`` honored, sub-dependency cache per job) and
    ``JobContext`` via ``Depends(current_job)``. Request-only dependencies raise a clear
    error. :meth:`lifespan` enters the app's lifespan once per worker process.
    """

    def __init__(
        self,
        app: FastAPI,
        *,
        registry: Registry | None = None,
        config: QueueConfig | None = None,
    ) -> None:
        raise NotImplementedError

    @property
    def app(self) -> FastAPI:
        raise NotImplementedError

    @property
    def registry(self) -> Registry:
        raise NotImplementedError

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

    def job(self, func: Callable[..., Any] | None = None, /, **kwargs: Any) -> Any:
        raise NotImplementedError

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        raise NotImplementedError


__all__ = ["JobContext", "LaravelCloudQueues", "current_job"]
