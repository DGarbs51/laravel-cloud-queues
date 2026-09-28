"""Each flagged line must produce exactly the ty error codes in its ``# E:`` marker."""

from __future__ import annotations

from laravel_cloud_queues import JobContext, Registry

registry = Registry()


@registry.job
async def send_email(user_id: int, template: str = "welcome") -> None: ...


@registry.job(name="reports.build")
def build_report(queue: str, delay: int, timeout: float) -> int:
    return delay


@registry.job(name="ctx")
def with_context(order_id: int, context: JobContext) -> None: ...


async def direct_calls() -> str:
    await send_email("1")  # E: invalid-argument-type
    build_report("q", 1)  # E: missing-argument
    return build_report("q", 1, 2.0)  # E: invalid-return-type


def dispatch_arguments() -> None:
    send_email.dispatch("1")  # E: invalid-argument-type
    send_email.dispatch()  # E: missing-argument
    send_email.dispatch(1, extra=2)  # E: unknown-argument
    send_email.dispatch(user_id=1, template=2)  # E: invalid-argument-type
    build_report.dispatch(queue=1, delay=60, timeout=1.5)  # E: invalid-argument-type
    build_report.dispatch(queue="q", delay="soon", timeout=1.5)  # E: invalid-argument-type


async def dispatch_async_arguments() -> None:
    await send_email.dispatch_async("1")  # E: invalid-argument-type
    await send_email.dispatch_async()  # E: missing-argument
    await send_email.options(queue="q").dispatch_async(1, nope=True)  # E: unknown-argument


def option_types() -> None:
    send_email.options(delay="x")  # E: invalid-argument-type
    send_email.options(queue=1)  # E: invalid-argument-type
    send_email.options(group=1)  # E: invalid-argument-type
    send_email.options(deduplication_id=b"id")  # E: invalid-argument-type
    send_email.options(message_group=["t"])  # E: invalid-argument-type
    send_email.options(priority=1)  # E: unknown-argument
    send_email.options("q")  # E: too-many-positional-arguments
    send_email.options(queue="q").dispatch("1")  # E: invalid-argument-type


def injected_parameters() -> None:
    # ParamSpec keeps the whole handler signature, so a JobContext parameter is required
    # statically (and rejected at run time with ArgumentError). Use current_job() inside the
    # handler instead when the job is dispatched from typed code.
    with_context.dispatch(1)  # E: missing-argument
    with_context.dispatch("1", context=None)  # E: invalid-argument-type, invalid-argument-type


def receipts() -> None:
    receipt = send_email.dispatch(1)
    _ = receipt.result  # E: unresolved-attribute
    receipt.queue = "x"  # E: invalid-assignment
