"""OpenTelemetry propagation resets between jobs (PROJECT_SCOPE.md §16)."""

from __future__ import annotations

import sys

import pytest

from laravel_cloud_queues.observability import activate_trace_context, inject_trace_context

pytest.importorskip("opentelemetry.sdk.trace")

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


def _install_provider() -> None:
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        trace.set_tracer_provider(TracerProvider())


def _carriers() -> tuple[dict[str, str], dict[str, str]]:
    _install_provider()
    tracer = trace.get_tracer("laravel_cloud_queues.test")
    with tracer.start_as_current_span("job-a"):
        carrier_a = inject_trace_context()
    with tracer.start_as_current_span("job-b"):
        carrier_b = inject_trace_context()
    return carrier_a, carrier_b


def test_inject_trace_context_writes_w3c_traceparent() -> None:
    carrier_a, carrier_b = _carriers()
    assert carrier_a["traceparent"] != carrier_b["traceparent"]
    assert carrier_a["traceparent"].startswith("00-")
    outside = inject_trace_context()
    assert "traceparent" not in outside or outside["traceparent"] != carrier_a["traceparent"]


def test_sequential_activations_reset_the_span_context() -> None:
    """Two jobs in a row must not leak span context.

    Conformance target: extract + attach for the job, detach afterwards
    (PROJECT_SCOPE.md §16).
    """

    carrier_a, carrier_b = _carriers()
    assert not trace.get_current_span().get_span_context().is_valid

    with activate_trace_context(carrier_a):
        current = trace.get_current_span().get_span_context()
        assert current.is_valid
        assert f"{current.trace_id:032x}" in carrier_a["traceparent"]
    assert not trace.get_current_span().get_span_context().is_valid

    with activate_trace_context(carrier_b):
        current = trace.get_current_span().get_span_context()
        assert current.is_valid
        assert f"{current.trace_id:032x}" in carrier_b["traceparent"]
        assert f"{current.trace_id:032x}" not in carrier_a["traceparent"]
    reset = trace.get_current_span().get_span_context()
    assert not reset.is_valid
    assert reset.span_id == 0


def test_activation_detaches_when_the_job_raises() -> None:
    carrier_a, _carrier_b = _carriers()
    with pytest.raises(RuntimeError, match="from job"), activate_trace_context(carrier_a):
        raise RuntimeError("from job")
    assert not trace.get_current_span().get_span_context().is_valid


def test_inject_swallows_propagator_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def explode(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("propagator down")

    monkeypatch.setattr("opentelemetry.propagate.inject", explode)
    assert inject_trace_context() == {}


def test_tracing_is_a_noop_when_opentelemetry_cannot_be_imported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    monkeypatch.setitem(sys.modules, "opentelemetry.context", None)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", None)
    assert inject_trace_context() == {}
    ran = False
    with activate_trace_context({"traceparent": "00-1"}):
        ran = True
    assert ran
