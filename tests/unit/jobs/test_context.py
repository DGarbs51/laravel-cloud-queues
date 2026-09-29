"""JobContext exactly-once semantics (D13.9)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from laravel_cloud_queues.errors import InvalidQueueOptionError, JobFailedError
from laravel_cloud_queues.jobs.context import ChosenOutcome, JobContext, JobControl, current_job


def make_context() -> JobContext:
    return JobContext(
        job_name="emails.send", uuid="u-1", message_id="m-1", queue="emails", attempt=2, max_tries=3
    )


def test_properties() -> None:
    context = make_context()
    assert (context.job_name, context.uuid, context.message_id) == ("emails.send", "u-1", "m-1")
    assert (context.queue, context.attempt, context.max_tries) == ("emails", 2, 3)
    assert context.outcome is None


def test_release_records_rounded_delay() -> None:
    context = make_context()
    with pytest.raises(JobControl):
        context.release(timedelta(seconds=2.1))
    assert context.outcome == ChosenOutcome("release", delay=3)


def test_release_clamps_to_twelve_hours() -> None:
    context = make_context()
    with pytest.raises(JobControl):
        context.release(10**6)
    assert context.outcome == ChosenOutcome("release", delay=43_200)


def test_release_with_invalid_delay_records_nothing() -> None:
    context = make_context()
    with pytest.raises(InvalidQueueOptionError):
        context.release(-1)
    assert context.outcome is None


@pytest.mark.parametrize(
    ("reason", "expected_type", "message"),
    [
        (None, JobFailedError, "Job failed explicitly."),
        ("bad input", JobFailedError, "bad input"),
        (ValueError("boom"), ValueError, "boom"),
    ],
)
def test_fail_reasons(reason: str | Exception | None, expected_type: type, message: str) -> None:
    context = make_context()
    with pytest.raises(JobControl):
        context.fail(reason)
    outcome = context.outcome
    assert outcome is not None
    assert outcome.kind == "fail"
    assert type(outcome.reason) is expected_type
    assert str(outcome.reason) == message
    if isinstance(reason, Exception):
        assert outcome.reason is reason


def test_first_call_wins() -> None:
    context = make_context()
    with pytest.raises(JobControl):
        context.release(5)
    with pytest.raises(JobControl):
        context.fail("later")
    with pytest.raises(JobControl):
        context.release(-1)  # not even validated once an outcome exists
    assert context.outcome == ChosenOutcome("release", delay=5)

    other = make_context()
    with pytest.raises(JobControl):
        other.fail()
    with pytest.raises(JobControl):
        other.release(1)
    assert other.outcome is not None
    assert other.outcome.kind == "fail"


def test_job_control_is_an_exception() -> None:
    """Handlers that ``except Exception`` swallow it; the recorded outcome still wins (see
    test_execution)."""
    assert issubclass(JobControl, Exception)


def test_current_job_outside_a_job() -> None:
    with pytest.raises(RuntimeError):
        current_job()


def test_repr_names_the_delivery_without_the_message_id() -> None:
    assert repr(make_context()) == (
        "JobContext(job_name='emails.send', uuid='u-1', queue='emails', attempt=2, max_tries=3)"
    )
