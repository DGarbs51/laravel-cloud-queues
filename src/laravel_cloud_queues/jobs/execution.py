"""The execution path shared by the worker and eager mode.

:func:`prepare_execution` performs every deterministic check: decoding the envelope, looking
up the job and decoding and validating its arguments. Any failure is a
:class:`JobDefectError`, which is terminal on the first delivery. :func:`run_prepared`
activates the trace context, sets the current job context, calls the invoker and maps the
result to exactly one :class:`HandlerResult`. It never touches the transport.
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
    """A decoded and validated job that is ready to be run."""

    envelope: Envelope
    """The decoded envelope of the message."""
    job: AnyJob
    """The registered job the message targets."""
    args: tuple[object, ...]
    """The decoded positional arguments."""
    kwargs: dict[str, object]
    """The decoded keyword arguments."""

    def policy(self, defaults: WorkerDefaults) -> ResolvedPolicy:
        """Get the message's retry policy, with worker defaults for any omitted fields."""
        return self.envelope.policy.resolve(defaults)


@dataclass(frozen=True)
class HandlerResult:
    """The result of running a job's handler."""

    kind: Literal["success", "release", "fail", "error"]
    """The kind of result.

    A ``release`` or ``fail`` is an outcome chosen explicitly through the job context, and
    it wins over both success and an exception. An ``error`` is an exception from the
    handler or its teardown, to which the retry policy applies.
    """
    delay: int = 0
    """The number of seconds to wait before a release."""
    exception: BaseException | None = None
    """The exception that failed the job or was raised by the handler."""


def prepare_execution(registry: Registry, body: str) -> PreparedExecution:
    """Prepare the given message body for execution.

    Raises a :class:`JobDefectError` if the body cannot be decoded, the job is unknown, or
    the arguments do not match the handler.
    """
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
    """Run the prepared job with the given context.

    Handler exceptions are never raised and become an ``error`` result instead. A
    ``BaseException`` that is not an ``Exception``, such as ``KeyboardInterrupt``,
    ``SystemExit`` or cancellation, propagates.
    """
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
