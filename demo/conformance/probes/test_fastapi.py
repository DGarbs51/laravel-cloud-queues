"""FastAPI-only contracts (§17, §20) have no PHP dependency-injection equivalent."""

from __future__ import annotations

import anyio
import pytest
from fastapi import Depends, FastAPI

from laravel_cloud_queues.config import QueueConfig, SqsConnectionConfig, StaticCredentials
from laravel_cloud_queues.fastapi import LaravelCloudQueues, current_job


@pytest.mark.conformance("fastapi.depends", tier="unit")
def test_depends_and_eager(evidence):
    app = FastAPI()
    config = QueueConfig(
        mode="sqs",
        sqs=SqsConnectionConfig(
            prefix="http://unused/123",
            region="us-east-1",
            credentials=StaticCredentials("test", "test"),
        ),
    )
    queues = LaravelCloudQueues(app, config=config)
    events = []

    def original():
        raise AssertionError("dependency override ignored")

    def dependency():
        events.append("enter")
        try:
            yield "resource"
        finally:
            events.append("exit")

    app.dependency_overrides[original] = dependency

    @queues.job(name="probe.depends")
    async def job(value: int, one: str = Depends(original), two: str = Depends(original)) -> None:
        assert one == two == "resource"
        assert current_job().attempt == 1
        events.append(value)
        if value == 3:
            raise ValueError("synthetic handler failure")

    with queues.registry.testing(eager=True):
        job.dispatch(value=1)

        async def in_loop():
            await job.dispatch_async(value=2)
            with pytest.raises(ValueError, match="synthetic"):
                await job.dispatch_async(value=3)

        anyio.run(in_loop)
    assert events == ["enter", 1, "exit", "enter", 2, "exit", "enter", 3, "exit"]
    with pytest.raises(RuntimeError):
        current_job()
    evidence.record("dependency_sequence", events)


@pytest.mark.conformance("tracing.propagation", tier="unit")
def test_tracing_optional(evidence):
    import subprocess
    import sys

    from laravel_cloud_queues.observability import activate_trace_context, inject_trace_context

    context = {"traceparent": "00-11111111111111111111111111111111-0123456789abcdef-01"}
    before = inject_trace_context()
    with activate_trace_context(context):
        assert inject_trace_context()["traceparent"] == context["traceparent"]
    assert inject_trace_context() == before
    # A fresh interpreter with OpenTelemetry imports denied exercises the optional path.
    code = """
import sys
class WithoutOTel:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "opentelemetry" or fullname.startswith("opentelemetry."):
            raise ModuleNotFoundError(fullname)
sys.meta_path.insert(0, WithoutOTel())
from laravel_cloud_queues.observability import inject_trace_context, activate_trace_context
assert inject_trace_context() == {}
with activate_trace_context({"traceparent": "invalid"}):
    assert inject_trace_context() == {}
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr
    evidence.record("traceparent", context["traceparent"])
    evidence.record("without_otel_exit", result.returncode)
