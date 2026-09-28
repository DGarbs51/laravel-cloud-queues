"""Job registry, invoker hook and worker target (PROJECT_SCOPE.md §7, §18).
CONTRACT — implemented by lane L3c."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import threading
from collections import OrderedDict
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, overload, runtime_checkable

from typing_extensions import ParamSpec

from ..codecs import CodecRegistry, default_codecs
from ..config import DEFAULT_QUEUE, QueueConfig, load_config
from ..errors import ConfigurationError, UnknownJobError
from ..jobs.context import JobContext
from ..jobs.job import AnyJob, Job
from ..jobs.policy import RetryPolicy
from ..observability import NullSink, SocketEventSink, Telemetry
from ..transports import SQS_MAX_PAYLOAD_BYTES, Backend, create_backend

if TYPE_CHECKING:
    from ..testing import DispatchRecorder, _TestingSession

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


class DefaultInvoker:
    """Core invoker: parameters annotated exactly ``JobContext`` receive the delivery's
    context; the handler is called on the current thread and awaited if it returns an
    awaitable (async handlers)."""

    def is_injected(self, parameter: inspect.Parameter) -> bool:
        # inspect_handler passes parameters with resolved type hints.
        return parameter.annotation is JobContext

    async def invoke(
        self,
        job: AnyJob,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        context: JobContext,
    ) -> None:
        signature = job._signature
        injected = {name: context for name in signature.injected}
        call_args, call_kwargs = merge_injected(signature.signature, args, kwargs, injected)
        result = job.func(*call_args, **call_kwargs)
        if inspect.isawaitable(result):
            await result


def merge_injected(
    signature: inspect.Signature,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    injected: Mapping[str, object],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Combine payload arguments (bound against the serialized parameters only) with
    injected values into ``(args, kwargs)`` for the full handler signature."""
    serialized = signature.replace(
        parameters=[p for p in signature.parameters.values() if p.name not in injected]
    )
    arguments = OrderedDict(serialized.bind(*args, **kwargs).arguments)
    positional = [
        p for p in signature.parameters.values() if p.kind is inspect.Parameter.POSITIONAL_ONLY
    ]
    if any(p.name in injected for p in positional):
        last_injected = max(i for i, p in enumerate(positional) if p.name in injected)
        for parameter in positional[:last_injected]:
            if parameter.name not in arguments and parameter.name not in injected:
                arguments[parameter.name] = parameter.default
    arguments.update(injected)
    bound = inspect.BoundArguments(signature, arguments)
    return bound.args, bound.kwargs


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
        backend: Backend | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        """``config`` defaults to :func:`load_config` resolved lazily on first dispatch or
        worker start. ``include``: modules imported by the worker at start (canonical).
        ``discover``: packages walked for job modules (opt-in convenience). ``backend`` /
        ``telemetry`` override the ones built from ``config`` (tests, harnesses)."""
        self._config = config
        self._codecs = codecs
        self._invoker: Invoker = invoker or DefaultInvoker()
        self._include = tuple(include)
        self._discover = tuple(discover)
        self._backend = backend
        self._telemetry = telemetry
        self._jobs: dict[str, AnyJob] = {}
        self._loaded = False
        self._lock = threading.RLock()
        self._testing_session: _TestingSession | None = None

    @property
    def registry(self) -> Registry:
        return self

    @property
    def config(self) -> QueueConfig:
        if self._config is None:
            with self._lock:
                if self._config is None:
                    self._config = load_config()
        return self._config

    @property
    def codecs(self) -> CodecRegistry:
        if self._codecs is None:
            with self._lock:
                if self._codecs is None:
                    self._codecs = default_codecs()
        return self._codecs

    @property
    def backend(self) -> Backend:
        """Lazily ``create_backend(self.config)`` (thread-safe, once per process)."""
        if self._backend is None:
            with self._lock:
                if self._backend is None:
                    self._backend = create_backend(self.config)
        return self._backend

    @property
    def telemetry(self) -> Telemetry:
        """Lazily built: socket sink on ``config.log_socket`` in managed mode, else a no-op
        sink (D12). Stdout failure lines are written in every mode."""
        if self._telemetry is None:
            with self._lock:
                if self._telemetry is None:
                    config = self.config
                    if config.emits_cloud_events:
                        self._telemetry = Telemetry(
                            sink=SocketEventSink(config.log_socket), emits_cloud_events=True
                        )
                    else:
                        self._telemetry = Telemetry(sink=NullSink(), emits_cloud_events=False)
        return self._telemetry

    @property
    def invoker(self) -> Invoker:
        return self._invoker

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
        fields override ``policy`` fields.

        Parameters annotated ``JobContext`` are injected at run time, but they stay in the
        static ``dispatch`` signature (ParamSpec cannot drop them), so typed callers could
        not omit them. The typed pattern is to call :func:`current_job` inside the handler
        (``Depends(current_job)`` with FastAPI) instead of declaring the parameter."""
        base = policy or RetryPolicy()
        effective = RetryPolicy(
            tries=base.tries if tries is None else tries,
            backoff=base.backoff if backoff is None else backoff,
            timeout=base.timeout if timeout is None else timeout,
            fail_on_timeout=base.fail_on_timeout if fail_on_timeout is None else fail_on_timeout,
        )

        def register(handler: Callable[..., Any]) -> AnyJob:
            job_name = f"{handler.__module__}.{handler.__qualname__}" if name is None else name
            if not job_name:
                raise ConfigurationError("Job names must be non-empty.")
            with self._lock:
                if job_name in self._jobs:
                    raise ConfigurationError(f"A job named [{job_name}] is already registered.")
                job = Job(handler, registry=self, name=job_name, queue=queue, policy=effective)
                self._jobs[job_name] = job
            return job

        return register if func is None else register(func)

    def get(self, name: str) -> AnyJob:
        """Registry lookup only; unknown -> UnknownJobError. Never imports anything."""
        try:
            return self._jobs[name]
        except KeyError:
            raise UnknownJobError(name) from None

    def jobs(self) -> Mapping[str, AnyJob]:
        return MappingProxyType(self._jobs)

    def load(self) -> None:
        """Import configured ``include`` modules and walk ``discover`` packages (idempotent).
        Driven only by application configuration, never by message content."""
        with self._lock:
            if self._loaded:
                return
            for module in self._include:
                if isinstance(module, str):
                    importlib.import_module(module)
            for package_name in self._discover:
                package = importlib.import_module(package_name)
                for info in pkgutil.walk_packages(
                    getattr(package, "__path__", ()), prefix=f"{package.__name__}."
                ):
                    importlib.import_module(info.name)
            self._loaded = True

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        """No-op for plain registries."""
        return _noop_lifespan()

    def testing(self, *, eager: bool = True) -> AbstractContextManager[DispatchRecorder]:
        """Swap dispatch for the test double. ``eager=True`` runs each dispatched job
        immediately through the worker's execution path (encode -> decode -> validate ->
        invoke), surfacing handler exceptions; ``eager=False`` only records."""
        from ..testing import _testing_session

        return _testing_session(self, eager=eager)

    def _default_queue(self) -> str:
        """Dispatch fallback queue. The test double never loads configuration implicitly."""
        if self._testing_session is not None and self._config is None:
            return DEFAULT_QUEUE
        return self.config.default_queue

    def _producer_capabilities(self) -> tuple[bool, int | None]:
        """``(supports_fifo, max_payload_bytes)`` of the dispatch producer. The test double
        never builds a backend implicitly: it follows the configured mode, else SQS rules."""
        if self._testing_session is None or self._backend is not None:
            producer = self.backend.producer
            return producer.supports_fifo, producer.max_payload_bytes
        if self._config is not None and self._config.mode == "redis":
            return False, None
        return True, SQS_MAX_PAYLOAD_BYTES


@asynccontextmanager
async def _noop_lifespan() -> AsyncIterator[None]:
    yield


__all__ = ["DefaultInvoker", "Invoker", "Registry", "WorkerTarget"]
