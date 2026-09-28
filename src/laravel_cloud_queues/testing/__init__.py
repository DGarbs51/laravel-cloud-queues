"""The testing primitives for application code.

Eager mode exercises argument binding, payload validation, serialization and
deserialization, registration and (with the FastAPI invoker) dependency injection and
cleanup through the worker's execution path. It does not replace transport or conformance
testing.

``registry.testing()`` records every dispatch (the real encoded body) and never touches the
backend or loads configuration implicitly: the default queue is ``config.default_queue``
when a config was given, else ``default``; option validation follows the given backend or
config mode, else SQS rules (1 MiB limit, FIFO/fair options allowed).

With ``eager=True`` each dispatch runs immediately as delivery attempt 1 through
``prepare_execution`` and ``run_prepared`` (the worker's path). A handler exception is
re-raised to the dispatching code, without applying the retry policy. Calling
``JobContext.fail()`` raises the failure reason (a ``JobFailedError`` or the given
exception). Calling ``JobContext.release()`` is recorded only, and the job is not re-run.
A job defect (e.g. an argument that does not round-trip) raises the ``JobDefectError``.

``dispatch_async`` awaits the job in the running loop. Sync ``dispatch`` uses ``anyio.run``
outside a loop; inside a running loop it runs the job on a helper thread with its own loop
(blocking the caller's loop until done, never nesting loops).
"""

from __future__ import annotations

import asyncio
import uuid as uuidlib
from collections.abc import Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio

from ..codecs import JSONValue
from ..jobs.context import JobContext
from ..jobs.envelope import decode_envelope
from ..jobs.execution import HandlerResult, PreparedExecution, prepare_execution, run_prepared
from ..jobs.job import DispatchReceipt
from ..jobs.policy import WorkerDefaults

if TYPE_CHECKING:
    from ..jobs.dispatch import PreparedDispatch
    from ..registry import Registry


@dataclass(frozen=True)
class RecordedDispatch:
    """A dispatch recorded by the test double."""

    job_name: str
    """The name of the dispatched job."""
    queue: str
    """The logical name of the queue the job was dispatched to."""
    body: str
    """The real encoded message body."""
    args: tuple[JSONValue, ...]
    """The positional arguments decoded from the body."""
    kwargs: Mapping[str, JSONValue]
    """The keyword arguments decoded from the body."""
    delay_seconds: int = 0
    """The number of whole seconds the message was delayed."""
    fifo_group: str | None = None
    """The ``MessageGroupId`` for ``.fifo`` queues, if any."""
    deduplication_id: str | None = None
    """The deduplication identifier of the dispatch, if any."""
    message_group: str | None = None
    """The fair-queue ``MessageGroupId`` on standard queues, if any."""


@dataclass
class DispatchRecorder:
    """The recorder of every dispatch made within a ``registry.testing()`` block."""

    dispatched: list[RecordedDispatch] = field(default_factory=list)
    """The recorded dispatches, in the order they were made."""

    def for_job(self, job_name: str) -> list[RecordedDispatch]:
        """Get the recorded dispatches of the job with the given name."""
        return [d for d in self.dispatched if d.job_name == job_name]


@dataclass
class _TestingSession:
    """The active ``registry.testing()`` block.

    The ``Job.dispatch*`` methods route here instead of sending.
    """

    registry: Registry
    """The registry whose dispatches are intercepted."""
    recorder: DispatchRecorder
    """The recorder receiving every dispatch."""
    eager: bool
    """Indicates if dispatched jobs should run immediately."""

    def dispatch(self, prepared: PreparedDispatch) -> DispatchReceipt:
        """Record the given dispatch and, in eager mode, run the job.

        Inside a running event loop the job runs on a helper thread with its own loop.
        """
        receipt = self._record(prepared)
        if self.eager:
            execution, context = self._start(prepared, receipt)

            def run() -> HandlerResult:
                """Run the prepared job in a new event loop."""
                return anyio.run(run_prepared, execution, context, backend="asyncio")

            try:
                asyncio.get_running_loop()
            except RuntimeError:
                result = run()
            else:
                with ThreadPoolExecutor(1, thread_name_prefix="lcq-eager") as pool:
                    result = pool.submit(run).result()
            _raise_failure(result)
        return receipt

    async def dispatch_async(self, prepared: PreparedDispatch) -> DispatchReceipt:
        """Record the given dispatch and, in eager mode, await the job in the running loop."""
        receipt = self._record(prepared)
        if self.eager:
            _raise_failure(await run_prepared(*self._start(prepared, receipt)))
        return receipt

    def _record(self, prepared: PreparedDispatch) -> DispatchReceipt:
        """Record the given dispatch and create its receipt."""
        message = prepared.message
        envelope = decode_envelope(message.body)
        self.recorder.dispatched.append(
            RecordedDispatch(
                job_name=prepared.job_name,
                queue=message.queue,
                body=message.body,
                args=envelope.args,
                kwargs=envelope.kwargs,
                delay_seconds=message.delay_seconds,
                fifo_group=message.fifo_group,
                deduplication_id=message.deduplication_id,
                message_group=message.message_group,
            )
        )
        return DispatchReceipt(
            message_id=f"eager-{uuidlib.uuid4()}", queue=message.queue, uuid=prepared.uuid
        )

    def _start(
        self, prepared: PreparedDispatch, receipt: DispatchReceipt
    ) -> tuple[PreparedExecution, JobContext]:
        """Prepare the dispatched job for execution as delivery attempt 1."""
        execution = prepare_execution(self.registry, prepared.message.body)
        context = JobContext(
            job_name=execution.job.name,
            uuid=execution.envelope.uuid,
            message_id=receipt.message_id,
            queue=receipt.queue,
            attempt=1,
            max_tries=execution.policy(WorkerDefaults()).tries,
        )
        return execution, context


def _raise_failure(result: HandlerResult) -> None:
    """Raise the exception of the given handler result if the job errored or failed."""
    if result.kind in ("error", "fail") and result.exception is not None:
        raise result.exception


@contextmanager
def _testing_session(registry: Registry, *, eager: bool) -> Iterator[DispatchRecorder]:
    """Route the registry's dispatches to a new testing session for the block.

    The previous session is restored on exit, so sessions may be nested.
    """
    session = _TestingSession(registry, DispatchRecorder(), eager)
    previous, registry._testing_session = registry._testing_session, session
    try:
        yield session.recorder
    finally:
        registry._testing_session = previous


__all__ = ["DispatchRecorder", "RecordedDispatch"]
