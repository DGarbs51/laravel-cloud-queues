"""The typed job object that wraps a handler function."""

from __future__ import annotations

import copy
import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

import anyio.to_thread
from typing_extensions import ParamSpec

from .policy import RetryPolicy
from .signature import JobSignature, inspect_handler

if TYPE_CHECKING:
    from ..registry import Registry

P = ParamSpec("P")
"""The parameters of the wrapped handler."""
R = TypeVar("R", covariant=True)
"""The return type of the wrapped handler."""


@dataclass(frozen=True)
class DispatchReceipt:
    """The receipt returned after a job has been dispatched."""

    message_id: str
    """The identifier assigned to the message by the transport."""
    queue: str
    """The logical name of the queue the message was sent to."""
    uuid: str
    """The ``uuid`` of the envelope, matching Laravel's payload key."""


@dataclass(frozen=True)
class DispatchOptions:
    """The options that control how a job is dispatched."""

    queue: str | None = None
    """The name of the queue to dispatch the job to."""
    delay: float | timedelta | None = None
    """The time to wait before the job becomes available, in seconds or as a timedelta."""
    group: str | None = None
    """The FIFO ``MessageGroupId``, for ``.fifo`` queues only."""
    deduplication_id: str | None = None
    """The FIFO deduplication ID.

    An empty string omits the attribute so the queue uses content-based deduplication.
    """
    message_group: str | None = None
    """The fair-queue tenant key, for standard queues only."""


class Job(Generic[P, R]):
    """A queued job that wraps a handler function.

    Calling the job invokes the handler directly, while ``dispatch`` and ``dispatch_async``
    take exactly the handler's parameters, so dispatch options never collide with handler
    parameters named ``queue``, ``delay`` or ``timeout``.

    A ``JobContext`` handler parameter is injected at run time but still appears in the typed
    ``dispatch`` and ``dispatch_async`` signatures. Typed code should call
    :func:`~laravel_cloud_queues.current_job` inside the handler instead.
    """

    def __init__(
        self,
        func: Callable[P, R],
        *,
        registry: Registry,
        name: str,
        queue: str | None,
        policy: RetryPolicy,
        options: DispatchOptions | None = None,
    ) -> None:
        """Create a new job instance."""
        self._func = func
        self._registry = registry
        self._name = name
        self._queue = queue
        self._policy = policy
        self._options = options or DispatchOptions()
        # Inspected once; ``.options()`` copies share it.
        self._signature = inspect_handler(
            func, is_injected=registry.invoker.is_injected, codecs=registry.codecs
        )
        functools.update_wrapper(self, func)

    def __repr__(self) -> str:
        """Get the string representation of the job."""
        return f"<Job {self._name!r}>"

    @property
    def name(self) -> str:
        """Get the name of the job."""
        return self._name

    @property
    def func(self) -> Callable[P, R]:
        """Get the handler function wrapped by the job."""
        return self._func

    @property
    def queue(self) -> str | None:
        """Get the default queue of the job, if one was declared."""
        return self._queue

    @property
    def policy(self) -> RetryPolicy:
        """Get the retry policy of the job."""
        return self._policy

    @property
    def registry(self) -> Registry:
        """Get the registry the job belongs to."""
        return self._registry

    @property
    def dispatch_options(self) -> DispatchOptions:
        """Get the options used when the job is dispatched."""
        return self._options

    @property
    def signature(self) -> JobSignature:
        """Get the inspected signature of the handler."""
        return self._signature

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Call the job's handler directly."""
        return self._func(*args, **kwargs)

    def call_bound(self, bound: inspect.BoundArguments) -> R:
        """Call the handler with arguments already bound to its signature.

        Invokers use this to run a delivery, since the payload arguments are decoded without
        the handler's static parameter types.
        """
        return self._func(*bound.args, **bound.kwargs)

    def options(
        self,
        *,
        queue: str | None = None,
        delay: float | timedelta | None = None,
        group: str | None = None,
        deduplication_id: str | None = None,
        message_group: str | None = None,
    ) -> Job[P, R]:
        """Get a copy of the job with the given dispatch options.

        Options are merged over any given to earlier ``options`` calls.
        """
        current = self._options
        clone = copy.copy(self)
        clone._options = DispatchOptions(
            queue=current.queue if queue is None else queue,
            delay=current.delay if delay is None else delay,
            group=current.group if group is None else group,
            deduplication_id=(
                current.deduplication_id if deduplication_id is None else deduplication_id
            ),
            message_group=current.message_group if message_group is None else message_group,
        )
        return clone

    def dispatch(self, *args: P.args, **kwargs: P.kwargs) -> DispatchReceipt:
        """Dispatch the job to the queue, blocking until it is sent.

        This still works inside a running event loop on this thread, since it never starts a
        nested loop, but it blocks that loop. Async code should use :meth:`dispatch_async`.
        """
        from .dispatch import prepare_dispatch, send_prepared

        prepared = prepare_dispatch(self, args, kwargs, self._options)
        session = self._registry.testing_session
        if session is not None:
            return session.dispatch(prepared)
        return send_prepared(self, prepared)

    async def dispatch_async(self, *args: P.args, **kwargs: P.kwargs) -> DispatchReceipt:
        """Dispatch the job to the queue without blocking the event loop.

        Every blocking call runs in a worker thread.
        """
        from .dispatch import prepare_dispatch, send_prepared

        session = self._registry.testing_session
        if session is not None:
            return await session.dispatch_async(prepare_dispatch(self, args, kwargs, self._options))
        # Preparing may build the configuration and backend lazily (blocking), so it shares
        # the worker-thread hop with the send.
        return await anyio.to_thread.run_sync(
            lambda: send_prepared(self, prepare_dispatch(self, args, kwargs, self._options))
        )


class AnyJob(Protocol):
    """A registered job of any handler signature, as seen by the worker and the registry.

    Every :class:`Job` satisfies this view, which erases the handler's parameter types.
    """

    @property
    def name(self) -> str:
        """Get the name of the job."""
        ...

    @property
    def func(self) -> object:
        """Get the handler function wrapped by the job."""
        ...

    @property
    def queue(self) -> str | None:
        """Get the default queue of the job, if one was declared."""
        ...

    @property
    def policy(self) -> RetryPolicy:
        """Get the retry policy of the job."""
        ...

    @property
    def registry(self) -> Registry:
        """Get the registry the job belongs to."""
        ...

    @property
    def signature(self) -> JobSignature:
        """Get the inspected signature of the handler."""
        ...

    def call_bound(self, bound: inspect.BoundArguments) -> object:
        """Call the handler with arguments already bound to its signature."""
        ...
