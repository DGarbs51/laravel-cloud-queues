"""Shared execution path for the worker and eager mode.

``prepare_execution`` performs every deterministic check (envelope decode, registry lookup,
argument decoding/validation); any failure is a JobDefectError (terminal on first delivery).
``run_prepared`` activates trace context, sets the current JobContext, calls the invoker and
maps the result to exactly one :class:`HandlerResult`. It never touches the transport.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ..errors import CodecError, JobDefectError
from ..observability import activate_trace_context
from ..registry import Registry
from .context import JobContext, _current_job
from .envelope import Envelope, decode_envelope
from .job import AnyJob
from .policy import ResolvedPolicy, WorkerDefaults
from .signature import decode_arguments


@dataclass(frozen=True)
class PreparedExecution:
    envelope: Envelope
    job: AnyJob
    args: tuple[object, ...]
    kwargs: dict[str, object]

    def policy(self, defaults: WorkerDefaults) -> ResolvedPolicy:
        """The message's policy (D4) with worker defaults for omitted fields."""
        return self.envelope.policy.resolve(defaults)


@dataclass(frozen=True)
class HandlerResult:
    kind: Literal["success", "release", "fail", "error"]
    """``release``/``fail``: explicit JobContext outcome (wins over success/exception).
    ``error``: handler or teardown exception (retry policy applies)."""
    delay: int = 0
    exception: BaseException | None = None


def prepare_execution(registry: Registry, body: str) -> PreparedExecution:
    envelope = decode_envelope(body)
    job = registry.get(envelope.job)
    try:
        args, kwargs = decode_arguments(
            job._signature, registry.codecs, envelope.args, envelope.kwargs
        )
    except JobDefectError:
        raise
    except Exception as exc:
        # A custom codec raising e.g. ValueError is still a deterministic defect of this body.
        raise CodecError(
            f"Arguments for job [{job.name}] could not be decoded: {type(exc).__name__}."
        ) from exc
    return PreparedExecution(envelope=envelope, job=job, args=tuple(args), kwargs=dict(kwargs))


async def run_prepared(prepared: PreparedExecution, context: JobContext) -> HandlerResult:
    """Never raises for handler exceptions (returns ``error``). ``BaseException`` that is
    not an ``Exception`` (KeyboardInterrupt, SystemExit, cancellation) propagates."""
    job = prepared.job
    error: Exception | None = None
    token = _current_job.set(context)
    try:
        with activate_trace_context(prepared.envelope.context):
            await job.registry.invoker.invoke(job, prepared.args, prepared.kwargs, context)
    except Exception as exc:
        error = exc
    finally:
        _current_job.reset(token)

    # A recorded outcome wins even if the handler swallowed JobControl or returned normally.
    outcome = context.outcome
    if outcome is not None:
        if outcome.kind == "release":
            return HandlerResult("release", delay=outcome.delay)
        return HandlerResult("fail", exception=outcome.reason)
    if error is not None:
        return HandlerResult("error", exception=error)
    return HandlerResult("success")
