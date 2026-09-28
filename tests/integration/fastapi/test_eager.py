"""Eager mode exercises FastAPI DI and teardown through the real registry."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import Depends, FastAPI

from laravel_cloud_queues.fastapi import LaravelCloudQueues, current_job
from laravel_cloud_queues.jobs.context import JobContext


def test_eager_dispatch_runs_dependencies_and_teardown() -> None:
    events: list[str] = []
    app = FastAPI()
    queues = LaravelCloudQueues(app)

    def mailer() -> Iterator[str]:
        events.append("enter")
        yield "smtp"
        events.append("exit")

    @queues.job(name="emails.send")
    def send_email(user_id: int, mailer_name: str = Depends(mailer)) -> str:
        events.append(f"run:{user_id}:{mailer_name}")
        return mailer_name

    with queues.registry.testing() as recorder:
        send_email.dispatch(1)

    assert events == ["enter", "run:1:smtp", "exit"]
    assert recorder.for_job("emails.send")


def test_eager_teardown_exception_surfaces_to_the_caller() -> None:
    app = FastAPI()
    queues = LaravelCloudQueues(app)

    def resource() -> Iterator[int]:
        yield 1
        raise RuntimeError("teardown boom")

    @queues.job
    def send_email(user_id: int, _value: int = Depends(resource)) -> None:
        assert user_id == 1

    with pytest.raises(RuntimeError, match="teardown boom"), queues.registry.testing():
        send_email.dispatch(1)


def test_eager_dispatch_injects_the_delivery_context() -> None:
    app = FastAPI()
    queues = LaravelCloudQueues(app)
    seen: list[tuple[int, str, int]] = []

    @queues.job(name="emails.send")
    def send_email(user_id: int, job: JobContext = Depends(current_job)) -> None:  # noqa: B008
        seen.append((user_id, job.job_name, job.attempt))

    with queues.registry.testing():
        send_email.dispatch(4)

    assert seen == [(4, "emails.send", 1)]


def test_direct_call_skips_dependency_injection() -> None:
    """Direct calls hit the raw function. Injected parameters are passed explicitly."""

    app = FastAPI()
    queues = LaravelCloudQueues(app)
    called = False

    def mailer() -> str:
        nonlocal called
        called = True
        return "smtp"

    @queues.job
    def send_email(user_id: int, mailer_name: str = Depends(mailer)) -> str:
        return f"{user_id}:{mailer_name}"

    assert send_email(1, mailer_name="explicit") == "1:explicit"
    assert called is False
