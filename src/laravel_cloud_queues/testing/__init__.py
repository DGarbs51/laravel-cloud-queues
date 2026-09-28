"""Testing primitives for application code (PROJECT_SCOPE.md §20). CONTRACT — lane L3c.

Eager mode exercises argument binding, payload validation, serialization/deserialization,
registration and (with the FastAPI invoker) DI + cleanup through the worker's execution
path. It does NOT replace transport or conformance testing.

``registry.testing()`` records every dispatch (the real encoded body) and never touches the
backend or loads configuration implicitly: the default queue is ``config.default_queue``
when a config was given, else ``default``; option validation follows the given backend or
config mode, else SQS rules (1 MiB limit, FIFO/fair options allowed).

With ``eager=True`` each dispatch runs immediately as delivery attempt 1 through
``prepare_execution`` + ``run_prepared`` (the worker's path):

- handler exception -> re-raised to the dispatching code (retry policy is not applied);
- ``JobContext.fail()`` -> the failure reason is raised (``JobFailedError`` or the given
  exception);
- ``JobContext.release()`` -> recorded only; the job is not re-run;
- a job defect (e.g. an argument that does not round-trip) -> the ``JobDefectError``.

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
    job_name: str
    queue: str
    body: str
    args: tuple[JSONValue, ...]
    kwargs: Mapping[str, JSONValue]
    delay_seconds: int = 0
    fifo_group: str | None = None
    deduplication_id: str | None = None
    message_group: str | None = None


@dataclass
class DispatchRecorder:
    dispatched: list[RecordedDispatch] = field(default_factory=list)

    def for_job(self, job_name: str) -> list[RecordedDispatch]:
        return [d for d in self.dispatched if d.job_name == job_name]


@dataclass
class _TestingSession:
    """Active ``registry.testing()`` block; ``Job.dispatch*`` route here instead of sending."""

    registry: Registry
    recorder: DispatchRecorder
    eager: bool

    def dispatch(self, prepared: PreparedDispatch) -> DispatchReceipt:
        receipt = self._record(prepared)
        if self.eager:
            execution, context = self._start(prepared, receipt)

            def run() -> HandlerResult:
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
        receipt = self._record(prepared)
        if self.eager:
            _raise_failure(await run_prepared(*self._start(prepared, receipt)))
        return receipt

    def _record(self, prepared: PreparedDispatch) -> DispatchReceipt:
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
    if result.kind in ("error", "fail") and result.exception is not None:
        raise result.exception


@contextmanager
def _testing_session(registry: Registry, *, eager: bool) -> Iterator[DispatchRecorder]:
    session = _TestingSession(registry, DispatchRecorder(), eager)
    previous, registry._testing_session = registry._testing_session, session
    try:
        yield session.recorder
    finally:
        registry._testing_session = previous


__all__ = ["DispatchRecorder", "RecordedDispatch"]
