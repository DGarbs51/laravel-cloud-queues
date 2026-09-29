"""The job registry, the handler invoker hook and the worker target protocol."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import threading
from collections import OrderedDict
from collections.abc import AsyncGenerator, Callable, Generator, Mapping, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    asynccontextmanager,
    contextmanager,
)
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING, Protocol, TypeVar, overload, runtime_checkable

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
    from ..testing import DispatchRecorder, TestingSession

P = ParamSpec("P")
"""The parameters of a registered handler."""
R = TypeVar("R")
"""The return type of a registered handler."""


class Invoker(Protocol):
    """The hook that determines how job handlers are called.

    By default, parameters annotated ``JobContext`` are injected, sync handlers run directly
    on the calling (main) thread and async handlers are awaited. The FastAPI adapter
    supplies an invoker with a per-job dependency scope.
    """

    def is_injected(self, parameter: inspect.Parameter) -> bool:
        """Determine if the given handler parameter is injected rather than serialized."""
        ...

    async def invoke(
        self,
        job: AnyJob,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        context: JobContext,
    ) -> None:
        """Run the handler and any per-job teardown.

        The teardown completes before returning, and a teardown exception propagates as a
        handler failure.
        """
        ...


@runtime_checkable
class WorkerTarget(Protocol):
    """The target run by ``laravel-cloud-queues work module:attr``.

    A :class:`Registry` is one; the FastAPI integration is another (resolved from
    ``app.state.laravel_cloud_queues``).
    """

    @property
    def registry(self) -> Registry:
        """Get the registry holding the target's jobs."""
        ...

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        """Get the lifespan context of the worker process.

        It is entered once per worker process and exited on clean termination.
        """
        ...


class DefaultInvoker:
    """The core invoker that calls handlers on the current thread.

    Parameters annotated exactly ``JobContext`` receive the delivery's context, and the
    handler is awaited if it returns an awaitable (async handlers).
    """

    def is_injected(self, parameter: inspect.Parameter) -> bool:
        """Determine if the given parameter is annotated exactly ``JobContext``."""
        # inspect_handler passes parameters with resolved type hints.
        return parameter.annotation is JobContext

    async def invoke(
        self,
        job: AnyJob,
        args: Sequence[object],
        kwargs: Mapping[str, object],
        context: JobContext,
    ) -> None:
        """Call the handler with the context injected, awaiting it when it is async."""
        signature = job.signature
        injected = {name: context for name in signature.injected}
        result = job.call_bound(bind_injected(signature.signature, args, kwargs, injected))
        if inspect.isawaitable(result):
            await result


def bind_injected(
    signature: inspect.Signature,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    injected: Mapping[str, object],
) -> inspect.BoundArguments:
    """Bind the payload arguments and the injected values to the full handler signature.

    The payload arguments are bound against the serialized parameters only. The result is
    passed to :meth:`Job.call_bound`.
    """
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
    return inspect.BoundArguments(signature, arguments)


def merge_injected(
    signature: inspect.Signature,
    args: Sequence[object],
    kwargs: Mapping[str, object],
    injected: Mapping[str, object],
) -> tuple[tuple[object, ...], dict[str, object]]:
    """Merge the payload arguments with the injected values as an ``(args, kwargs)`` pair.

    This is :func:`bind_injected` for callers that spread the arguments themselves.
    """
    bound = bind_injected(signature, args, kwargs, injected)
    return bound.args, bound.kwargs


class Registry:
    """A standalone job registry for plain Python apps and the core of every framework adapter."""

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
        """Create a new registry instance.

        The ``config`` defaults to :func:`load_config`, resolved lazily on first dispatch or
        worker start. The ``include`` modules are imported by the worker at start (canonical),
        while the ``discover`` packages are walked for job modules (opt-in convenience). The
        ``backend`` and ``telemetry`` override the ones built from ``config`` (tests, harnesses).
        """
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
        self._testing_session: TestingSession | None = None

    @property
    def registry(self) -> Registry:
        """Get the registry itself, so it may serve as a worker target."""
        return self

    @property
    def config(self) -> QueueConfig:
        """Get the queue configuration, loading it lazily on first access."""
        if self._config is None:
            with self._lock:
                if self._config is None:
                    self._config = load_config()
        return self._config

    @property
    def codecs(self) -> CodecRegistry:
        """Get the codec registry, building the default codecs lazily on first access."""
        if self._codecs is None:
            with self._lock:
                if self._codecs is None:
                    self._codecs = default_codecs()
        return self._codecs

    @property
    def backend(self) -> Backend:
        """Get the queue backend.

        The backend is created lazily from the configuration, once per process, and creation
        is thread-safe.
        """
        if self._backend is None:
            with self._lock:
                if self._backend is None:
                    self._backend = create_backend(self.config)
        return self._backend

    @property
    def telemetry(self) -> Telemetry:
        """Get the telemetry, building it lazily on first access.

        Managed mode uses a socket sink on ``config.log_socket``, while other modes use a
        no-op sink. Stdout failure lines are written in every mode.
        """
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
        """Get the invoker used to call job handlers."""
        return self._invoker

    @property
    def testing_session(self) -> TestingSession | None:
        """Get the active :meth:`testing` session that intercepts dispatches, if any."""
        return self._testing_session

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
        """Register the given handler as a job.

        The default wire name is ``module.qualname``, though an explicit ``name`` is preferred
        for refactor safety. Registering a duplicate or empty name raises a
        :class:`ConfigurationError`. The shorthand options override the ``policy`` fields.

        Parameters annotated ``JobContext`` are injected at run time, but they stay in the
        static ``dispatch`` signature (ParamSpec cannot drop them), so typed callers could
        not omit them. The typed pattern is to call :func:`current_job` inside the handler
        (``Depends(current_job)`` with FastAPI) instead of declaring the parameter.
        """
        base = policy or RetryPolicy()
        effective = RetryPolicy(
            tries=base.tries if tries is None else tries,
            backoff=base.backoff if backoff is None else backoff,
            timeout=base.timeout if timeout is None else timeout,
            fail_on_timeout=base.fail_on_timeout if fail_on_timeout is None else fail_on_timeout,
        )

        def register(handler: Callable[P, R]) -> Job[P, R]:
            """Register the handler under its job name."""
            if name is None:
                qualname = getattr(handler, "__qualname__", None)
                if qualname is None:
                    raise ConfigurationError("Callable objects need an explicit job name.")
                job_name = f"{handler.__module__}.{qualname}"
            else:
                job_name = name
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
        """Get the registered job with the given name.

        This is a registry lookup only and never imports anything. Raises an
        :class:`UnknownJobError` if no job has the name.
        """
        try:
            return self._jobs[name]
        except KeyError:
            raise UnknownJobError(name) from None

    def jobs(self) -> Mapping[str, AnyJob]:
        """Get a read-only view of the registered jobs, keyed by name."""
        return MappingProxyType(self._jobs)

    def load(self) -> None:
        """Import the ``include`` modules and walk the ``discover`` packages.

        Loading is idempotent, and it is driven only by application configuration, never by
        message content.
        """
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
        """Get the lifespan context of the worker process, which is a no-op here."""
        return _noop_lifespan()

    def testing(self, *, eager: bool = True) -> AbstractContextManager[DispatchRecorder]:
        """Swap dispatching for the test double within the returned context.

        When ``eager`` is true, each dispatched job runs immediately through the worker's
        execution path (encode, decode, validate, invoke), surfacing handler exceptions.
        Otherwise, dispatches are only recorded.
        """
        from ..testing import DispatchRecorder, TestingSession

        return self._activate(TestingSession(self, DispatchRecorder(), eager))

    @contextmanager
    def _activate(self, session: TestingSession) -> Generator[DispatchRecorder, None, None]:
        """Route dispatches to the session for the block, restoring the previous one on exit."""
        previous, self._testing_session = self._testing_session, session
        try:
            yield session.recorder
        finally:
            self._testing_session = previous

    def default_queue(self) -> str:
        """Get the queue that dispatches fall back to.

        The test double never loads configuration implicitly.
        """
        if self._testing_session is not None and self._config is None:
            return DEFAULT_QUEUE
        return self.config.default_queue

    def producer_capabilities(self) -> tuple[bool, int | None]:
        """Get the ``(supports_fifo, max_payload_bytes)`` capabilities of the dispatch producer.

        The test double never builds a backend implicitly: it follows the configured mode,
        else the SQS rules.
        """
        if self._testing_session is None or self._backend is not None:
            producer = self.backend.producer
            return producer.supports_fifo, producer.max_payload_bytes
        if self._config is not None and self._config.mode == "redis":
            return False, None
        return True, SQS_MAX_PAYLOAD_BYTES


@asynccontextmanager
async def _noop_lifespan() -> AsyncGenerator[None, None]:
    """Enter and exit an empty worker lifespan."""
    yield


__all__ = ["DefaultInvoker", "Invoker", "Registry", "WorkerTarget"]
