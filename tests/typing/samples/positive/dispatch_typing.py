"""Downstream usage that must pass ``ty`` (D5)."""

from __future__ import annotations

from collections.abc import Coroutine
from datetime import timedelta
from typing import Any, assert_type

from laravel_cloud_queues import DispatchReceipt, Job, JobContext, Registry, current_job

registry = Registry()


@registry.job
async def send_email(user_id: int, template: str = "welcome") -> None:
    context = current_job()  # injected at run time without a parameter
    assert_type(context, JobContext)


@registry.job(name="reports.build", queue="reports", tries=3, backoff=[1, 5], timeout=30)
def build_report(queue: str, delay: int, timeout: float) -> int:
    """Handler parameters may be named like dispatch options."""
    return delay


@registry.job(name="ctx")
def with_context(order_id: int, context: JobContext) -> None:
    context.release(timedelta(seconds=5))


def sync_usage() -> None:
    # Direct calls keep the handler's signature and return type.
    assert_type(build_report("q", 1, 2.0), int)
    coroutine: Coroutine[Any, Any, None] = send_email(1)
    coroutine.close()

    assert_type(send_email.dispatch(1), DispatchReceipt)
    assert_type(send_email.dispatch(user_id=1, template="reset"), DispatchReceipt)
    assert_type(build_report.dispatch(queue="other", delay=60, timeout=1.5), DispatchReceipt)

    delayed = send_email.options(queue="priority", delay=30)
    typed_copy: Job[..., Coroutine[Any, Any, None]] = delayed
    assert_type(typed_copy.name, str)
    delayed.options(delay=timedelta(seconds=2.5)).dispatch(2)
    send_email.options(queue="orders.fifo", group="customer-1", deduplication_id="").dispatch(3)
    send_email.options(message_group="tenant-9").dispatch(user_id=4)
    build_report.options(queue="reports-high", delay=0.5).dispatch("q", 1, 2.0)

    receipt = send_email.dispatch(5)
    assert_type(receipt.message_id, str)
    assert_type(receipt.queue, str)
    assert_type(receipt.uuid, str)

    assert_type(send_email.name, str)
    assert_type(send_email.queue, str | None)


async def async_usage() -> None:
    await send_email(1)
    assert_type(await send_email.dispatch_async(1), DispatchReceipt)
    assert_type(await send_email.options(delay=1).dispatch_async(user_id=2), DispatchReceipt)
    assert_type(await build_report.dispatch_async("q", delay=1, timeout=0.0), DispatchReceipt)
