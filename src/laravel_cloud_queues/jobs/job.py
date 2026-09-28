"""``Job`` — the typed, directly callable job object (PROJECT_SCOPE.md §7, §9, §10, D5).
CONTRACT — implemented by lane L3c."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import ParamSpec

from .policy import RetryPolicy

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
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def func(self) -> Callable[P, R]:
        raise NotImplementedError

    @property
    def queue(self) -> str | None:
        raise NotImplementedError

    @property
    def policy(self) -> RetryPolicy:
        raise NotImplementedError

    @property
    def registry(self) -> Registry:
        raise NotImplementedError

    @property
    def dispatch_options(self) -> DispatchOptions:
        raise NotImplementedError

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        raise NotImplementedError

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
        raise NotImplementedError

    def dispatch(self, *args: P.args, **kwargs: P.kwargs) -> DispatchReceipt:
        """Blocking dispatch. Inside a running event loop in this thread it still works (it
        never starts a nested loop) but blocks the loop; async code should use
        :meth:`dispatch_async`."""
        raise NotImplementedError

    async def dispatch_async(self, *args: P.args, **kwargs: P.kwargs) -> DispatchReceipt:
        """Non-blocking dispatch: every blocking call runs in a worker thread."""
        raise NotImplementedError


AnyJob = Job[..., Any]
