"""``Job`` — the typed, directly callable job object (PROJECT_SCOPE.md §7, §9, §10, D5).
CONTRACT — implemented by lane L3c."""

from __future__ import annotations

import copy
import functools
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import anyio.to_thread
from typing_extensions import ParamSpec

from .policy import RetryPolicy
from .signature import inspect_handler

if TYPE_CHECKING:
    from ..registry import Registry

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


@dataclass(frozen=True)
class DispatchReceipt:
    message_id: str
    queue: str
    """Logical queue name the message was sent to."""
    uuid: str
    """The envelope ``uuid`` (Laravel payload key)."""


@dataclass(frozen=True)
class DispatchOptions:
    queue: str | None = None
    delay: float | timedelta | None = None
    group: str | None = None
    """FIFO ``MessageGroupId`` (``.fifo`` queues only)."""
    deduplication_id: str | None = None
    """FIFO dedup ID; ``""`` = omit the attribute (content-based dedup)."""
    message_group: str | None = None
    """Fair-queue tenant key (standard queues only)."""


class Job(Generic[P, R]):
    """Wraps a handler. Calling it calls the handler directly; ``dispatch``/``dispatch_async``
    take exactly the handler's parameters, so options never collide with handler parameters
    named ``queue``/``delay``/``timeout``."""

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
        self._func = func
        self._registry = registry
        self._name = name
        self._queue = queue
        self._policy = policy
        self._options = options or DispatchOptions()
        # Inspected once; ``.options()`` copies share it.
        self._signature = inspect_handler(func, is_injected=registry.invoker.is_injected)
        functools.update_wrapper(self, func)

    def __repr__(self) -> str:
        return f"<Job {self._name!r}>"

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[P, R]:
        return self._func

    @property
    def queue(self) -> str | None:
        return self._queue

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def dispatch_options(self) -> DispatchOptions:
        return self._options

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._func(*args, **kwargs)

    def options(
        self,
        *,
        queue: str | None = None,
        delay: float | timedelta | None = None,
        group: str | None = None,
        deduplication_id: str | None = None,
        message_group: str | None = None,
    ) -> Job[P, R]:
        """Typed copy carrying dispatch options (merged over earlier ``.options()`` calls)."""
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
        """Blocking dispatch. Inside a running event loop in this thread it still works (it
        never starts a nested loop) but blocks the loop; async code should use
        :meth:`dispatch_async`."""
        from .dispatch import prepare_dispatch, send_prepared

        prepared = prepare_dispatch(self, args, kwargs, self._options)
        session = self._registry._testing_session
        if session is not None:
            return session.dispatch(prepared)
        return send_prepared(self, prepared)

    async def dispatch_async(self, *args: P.args, **kwargs: P.kwargs) -> DispatchReceipt:
        """Non-blocking dispatch: every blocking call runs in a worker thread."""
        from .dispatch import prepare_dispatch, send_prepared

        session = self._registry._testing_session
        if session is not None:
            return await session.dispatch_async(prepare_dispatch(self, args, kwargs, self._options))
        # Preparing may build the configuration and backend lazily (blocking), so it shares
        # the worker-thread hop with the send.
        return await anyio.to_thread.run_sync(
            lambda: send_prepared(self, prepare_dispatch(self, args, kwargs, self._options))
        )


AnyJob = Job[..., Any]
