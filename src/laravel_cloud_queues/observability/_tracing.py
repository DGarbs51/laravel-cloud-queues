"""The optional OpenTelemetry and W3C trace context propagation.

OpenTelemetry is imported inside the functions so core imports succeed without the
package. Failures are swallowed: tracing must not break queue execution, and a job's
context is always detached so it cannot leak into the next delivery.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager

from ._guard import log_failure


@contextmanager
def _noop_context() -> Iterator[None]:
    """Provide a context that does nothing."""
    yield


def inject_trace_context() -> dict[str, str]:
    """Get the current W3C ``traceparent`` and ``tracestate`` headers.

    Returns an empty dictionary when OpenTelemetry is not importable or injection fails.
    """

    try:
        from opentelemetry import propagate

        carrier: dict[str, str] = {}
        propagate.inject(carrier)
    except Exception:
        return {}
    return carrier


def activate_trace_context(carrier: Mapping[str, str]) -> AbstractContextManager[None]:
    """Activate the carrier's trace context for the duration of a job.

    The context is detached afterwards so it cannot leak into the next job. This does
    nothing without OpenTelemetry and never raises.
    """

    try:
        from opentelemetry import context as otel_context
        from opentelemetry import propagate
    except Exception:
        return _noop_context()

    @contextmanager
    def _activated() -> Iterator[None]:
        """Attach the extracted context and detach it on exit."""
        token = None
        try:
            try:
                extracted = propagate.extract(dict(carrier))
                token = otel_context.attach(extracted)
            except Exception:
                token = None
            yield
        finally:
            if token is not None:
                try:
                    otel_context.detach(token)
                except Exception:
                    log_failure("observability trace detach failed")

    return _activated()
