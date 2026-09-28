"""Job registry, invoker hook and worker target (PROJECT_SCOPE.md §7, §18).
CONTRACT — implemented by lane L3c."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload, runtime_checkable

from typing_extensions import ParamSpec

from ..codecs import CodecRegistry
from ..config import QueueConfig
from ..jobs.job import AnyJob, Job
from ..jobs.policy import RetryPolicy

if TYPE_CHECKING:
    from ..jobs.context import JobContext
    from ..testing import DispatchRecorder

P = ParamSpec("P")
R = TypeVar("R")


class Invoker(Protocol):
    """How handlers are called. Core default: parameters annotated ``JobContext`` are
    injected; sync handlers run directly on the calling (main) thread; async handlers are
    awaited. The FastAPI adapter supplies an invoker with a per-job dependency scope."""

    def is_injected(self, parameter: inspect.Parameter) -> bool: ...

    async def invoke(
        self,
        job: AnyJob,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        context: JobContext,
    ) -> None:
        """Run the handler and any per-job teardown; teardown completes before returning.
        A teardown exception propagates as a handler failure."""
        ...


@runtime_checkable
class WorkerTarget(Protocol):
    """What ``laravel-cloud-queues work module:attr`` runs. :class:`Registry` is one; the
    FastAPI integration is another (resolved from ``app.state.laravel_cloud_queues``)."""

    @property
    def registry(self) -> Registry: ...

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        """Entered once per worker process; exited on clean termination."""
        ...


class Registry:
    """Standalone registry for plain Python apps; the core of every framework adapter."""

    def __init__(
        self,
        *,
        config: QueueConfig | None = None,
        codecs: CodecRegistry | None = None,
        invoker: Invoker | None = None,
        include: Sequence[str | ModuleType] = (),
        discover: Sequence[str] = (),
    ) -> None:
        """``config`` defaults to :func:`load_config` resolved lazily on first dispatch or
        worker start. ``include``: modules imported by the worker at start (canonical).
        ``discover``: packages walked for job modules (opt-in convenience)."""
        raise NotImplementedError

    @property
    def registry(self) -> Registry:
        return self

    @property
    def config(self) -> QueueConfig:
        raise NotImplementedError

    @property
    def codecs(self) -> CodecRegistry:
        raise NotImplementedError

    @property
    def invoker(self) -> Invoker:
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

    def job(
        self,
        func: Callable[..., Any] | None = None,
        /,
        *,
        name: str | None = None,
        queue: str | None = None,
        tries: int | None = None,
        backoff: float | Sequence[float] | None = None,
        timeout: float | None = None,
        fail_on_timeout: bool | None = None,
        policy: RetryPolicy | None = None,
    ) -> Any:
        """Register a handler. Default wire name: ``module.qualname`` (explicit ``name``
        preferred for refactor safety). Duplicate names -> ConfigurationError. Shorthand
        fields override ``policy`` fields."""
        raise NotImplementedError

    def get(self, name: str) -> AnyJob:
        """Registry lookup only; unknown -> UnknownJobError. Never imports anything."""
        raise NotImplementedError

    def jobs(self) -> Mapping[str, AnyJob]:
        raise NotImplementedError

    def load(self) -> None:
        """Import configured ``include`` modules and walk ``discover`` packages (idempotent).
        Driven only by application configuration, never by message content."""
        raise NotImplementedError

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        """No-op for plain registries."""
        raise NotImplementedError

    def testing(self, *, eager: bool = True) -> AbstractContextManager[DispatchRecorder]:
        """Swap dispatch for the test double. ``eager=True`` runs each dispatched job
        immediately through the worker's execution path (encode -> decode -> validate ->
        invoke), surfacing handler exceptions; ``eager=False`` only records."""
        raise NotImplementedError


__all__ = ["Invoker", "Registry", "WorkerTarget"]
