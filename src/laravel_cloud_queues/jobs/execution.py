"""Shared execution path for the worker and eager mode (PROJECT_SCOPE.md §8, §12, §20).
CONTRACT — lane L3c.

``prepare_execution`` performs every deterministic check (envelope decode, registry lookup,
argument decoding/validation); any failure is a JobDefectError (terminal on first delivery).
``run_prepared`` activates trace context, sets the current JobContext, calls the invoker and
maps the result to exactly one :class:`HandlerResult`. It never touches the transport.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..registry import Registry
from .context import JobContext
from .envelope import Envelope
from .job import AnyJob
from .policy import ResolvedPolicy, WorkerDefaults


@dataclass(frozen=True)
class PreparedExecution:
    envelope: Envelope
    job: AnyJob
    args: tuple[object, ...]
    kwargs: dict[str, object]

    def policy(self, defaults: WorkerDefaults) -> ResolvedPolicy:
        """The message's policy (D4) with worker defaults for omitted fields."""
        raise NotImplementedError


@dataclass(frozen=True)
class HandlerResult:
    kind: Literal["success", "release", "fail", "error"]
    """``release``/``fail``: explicit JobContext outcome (wins over success/exception).
    ``error``: handler or teardown exception (retry policy applies)."""
    delay: int = 0
    exception: BaseException | None = None


def prepare_execution(registry: Registry, body: str) -> PreparedExecution:
    raise NotImplementedError


async def run_prepared(prepared: PreparedExecution, context: JobContext) -> HandlerResult:
    """Never raises for handler exceptions (returns ``error``). ``BaseException`` that is
    not an ``Exception`` (KeyboardInterrupt, SystemExit, cancellation) propagates."""
    raise NotImplementedError
