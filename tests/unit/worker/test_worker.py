"""Worker state machine, outcome and acknowledgement tables (docs/contract/worker.md).

Laravel references (pinned framework, ``src/Illuminate/``): pre-run check
``Queue/Worker.php:701-718``; retry/terminal after an exception ``Queue/Worker.php:648-687``
and ``:729-740``; backoff ``:817-827``; timeout handler ``:319-356``; stop conditions
``:419-432``; daemon loop order (job -> rest, empty -> sleep, then stop checks) ``:267-304``.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from typing import Any
from unittest.mock import Mock

import pytest
from laravel_cloud_logging import CloudHandler

import laravel_cloud_queues.worker as worker_module
from laravel_cloud_queues.errors import (
    AgentProtocolError,
    AgentUnavailableError,
    AmbiguousAcknowledgementError,
    BrokerConnectionError,
    ConfigurationError,
    JobFailedError,
    JobTimeoutError,
    LeaseLostError,
    MalformedEnvelopeError,
    ManagedQueueNotFoundError,
    MaxAttemptsExceededError,
    TransportError,
)
from laravel_cloud_queues.worker import EXIT_CONFIG, EXIT_FATAL, EXIT_OK, EXIT_TIMEOUT, Worker
from tests.unit.worker.doubles import Exited, Harness, delivery


def exit_code(h: Harness) -> int:
    """Run; a timeout surfaces as the patched ``os._exit`` (possibly inside a group)."""
    try:
        return h.run()
    except BaseException as exc:
        found = _find(exc)
        if found is None:
            raise
        return found.code


def _find(exc: BaseException) -> Exited | None:
    if isinstance(exc, Exited):
        return exc
    for inner in getattr(exc, "exceptions", ()):
        found = _find(inner)
        if found is not None:
            return found
    return None


def types_of(h: Harness) -> list[str]:
    return [str(e.get("type", e["_cloud_event"])) for e in h.events()]


# --- logging ------------------------------------------------------------------------------


@pytest.fixture
def bare_root(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Empty the root logger; ``configure()`` hooks are restored afterwards.

    Call it inside the test: pytest adds its capture handler to the root logger after setup.
    """
    monkeypatch.setattr(sys, "excepthook", sys.excepthook)
    monkeypatch.setattr(threading, "excepthook", threading.excepthook)
    monkeypatch.setattr(worker_module.anyio, "run", lambda *args, **kwargs: EXIT_OK)

    def empty() -> logging.Logger:
        root = logging.getLogger()
        monkeypatch.setattr(root, "handlers", [])
        monkeypatch.setattr(root, "level", logging.WARNING)
        return root

    return empty


def test_run_configures_laravel_cloud_logging_when_the_root_logger_is_bare(
    bare_root: Any,
) -> None:
    root = bare_root()
    try:
        assert Worker(Mock()).run() == EXIT_OK
        assert root.level == logging.INFO
        assert [type(handler) for handler in root.handlers] == [CloudHandler]
    finally:
        logging.captureWarnings(False)


def test_run_keeps_logging_the_app_configured(
    bare_root: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = bare_root()
    handler = logging.NullHandler()
    root.addHandler(handler)
    monkeypatch.setattr(worker_module, "configure", Mock())
    assert Worker(Mock()).run() == EXIT_OK
    worker_module.configure.assert_not_called()
    assert root.handlers == [handler]


# --- outcome table --------------------------------------------------------------------------


def test_success_completes_and_reports_processed(make: Any) -> None:
    d = delivery()
    h = make([d], mode="managed")
    assert h.run() == EXIT_OK
    assert h.env.of("complete") == [d.message_id]
    assert types_of(h) == ["started", "processed"]
    [line] = h.job_lines()
    assert line["status"] == "processed"
    assert line["job"] == "demo.job"
    assert line["message_id"] == d.message_id
    assert line["attempt"] == 1
    assert line["duration_ms"] >= 0
    assert "secret-receipt" not in repr(h.env.journal)
    assert h.env.lifespan == ["enter", "exit"]
    assert h.consumer.closed


def test_error_with_attempts_left_releases_same_message_with_backoff(make: Any) -> None:
    d = delivery("raise", tries=3, backoff=[7, 9])
    h = make([d], mode="managed")
    assert h.run() == EXIT_OK
    assert h.env.of("release") == [(d.message_id, 7)]
    assert h.env.of("complete") == []
    assert types_of(h) == ["started", "released"]
    assert h.job_lines()[0]["status"] == "released"


@pytest.mark.parametrize(
    ("attempt", "delay"), [(1, 1), (2, 6), (3, 6), (4, 6)], ids=["first", "second", "past", "far"]
)
def test_backoff_selection_last_value_repeats_and_rounds_up(
    make: Any, attempt: int, delay: int
) -> None:
    h = make([delivery("raise", attempt=attempt, tries=5, backoff=[1, 5.5])])
    h.run()
    assert h.env.of("release")[0][1] == delay


def test_default_tries_is_one_so_first_error_is_terminal(make: Any) -> None:
    """Laravel default ``tries`` is 1: the first exception fails the job."""
    d = delivery("raise")
    h = make([d], mode="managed")
    h.run()
    assert h.env.names() == ["event", "line", "complete", "event", "event", "line"]
    assert types_of(h) == ["started", "failed_job", "failed"]
    failed_job = h.events()[1]
    assert isinstance(failed_job["exception"], RuntimeError)
    assert failed_job["payload"] == d.body
    assert failed_job["queue"] == d.queue
    assert failed_job["attempts"] == 1
    assert failed_job["timestamp"] == h.events()[2]["timestamp"]


def test_tries_zero_is_unlimited(make: Any) -> None:
    h = make([delivery("raise", attempt=500, tries=0)])
    h.run()
    assert len(h.env.of("release")) == 1


def test_pre_run_check_fails_without_running(make: Any) -> None:
    d = delivery(attempt=3, tries=2)
    h = make([d], mode="managed")
    h.run()
    assert h.env.ran == []
    assert h.env.of("complete") == [d.message_id]
    assert isinstance(h.events()[1]["exception"], MaxAttemptsExceededError)
    assert types_of(h) == ["started", "failed_job", "failed"]


def test_last_attempt_runs(make: Any) -> None:
    h = make([delivery(attempt=2, tries=2)])
    h.run()
    assert h.env.ran == ["succeed"]


def test_explicit_release_wins_even_on_last_attempt(make: Any) -> None:
    """D13.9: explicit release always releases."""
    d = delivery("release:30", tries=1)
    h = make([d])
    h.run()
    assert h.env.of("release") == [(d.message_id, 30)]


def test_explicit_fail_is_terminal_with_attempts_left(make: Any) -> None:
    h = make([delivery("fail", tries=5)], mode="managed")
    h.run()
    assert len(h.env.of("complete")) == 1
    assert isinstance(h.events()[1]["exception"], JobFailedError)


def test_explicit_fail_keeps_the_given_exception(make: Any) -> None:
    h = make([delivery("fail:quota", tries=5)], mode="managed")
    h.run()
    exception = h.events()[1]["exception"]
    assert isinstance(exception, ValueError)
    assert str(exception) == "quota"


def test_job_defect_is_terminal_on_first_delivery(make: Any) -> None:
    d = delivery("defect", tries=5)
    h = make([d], mode="managed")
    h.run()
    assert h.env.ran == []
    assert h.env.of("complete") == [d.message_id]
    assert isinstance(h.events()[1]["exception"], MalformedEnvelopeError)
    assert h.job_lines()[0]["job"] is None


# --- modes: telemetry gating and D6b ---------------------------------------------------------


@pytest.mark.parametrize("mode", ["sqs", "redis"])
def test_direct_modes_emit_no_events_and_log_failure_record(make: Any, mode: str) -> None:
    d = delivery("raise")
    h = make([d], mode=mode)
    h.run()
    assert h.events() == []
    record, job_line = h.lines()
    assert record["laravel_cloud_queues"] == "failed_job"
    assert record["payload"] == d.body
    assert record["message_id"] == d.message_id
    assert job_line["status"] == "failed"
    assert h.env.names() == ["line", "complete", "line"]


TERMINAL = {
    "handler error on last attempt": {"do": "raise"},
    "explicit fail": {"do": "fail", "tries": 5},
    "pre-run exceeded": {"do": "succeed", "attempt": 3, "tries": 2},
    "job defect": {"do": "defect"},
}


def _terminal(case: str) -> Any:
    spec = dict(TERMINAL[case])
    return delivery(spec.pop("do"), attempt=spec.pop("attempt", 1), **spec)


def _order(h: Harness) -> list[str]:
    """Call order: transport calls, socket events by type, stdout lines by kind."""
    order = []
    for name, value in h.env.journal:
        if name == "event":
            order.append(str(value.get("type", value["_cloud_event"])))
        elif name == "line":
            order.append(f"line:{value['laravel_cloud_queues']}")
        else:
            order.append(name)
    return order


@pytest.mark.parametrize("mode", ["sqs", "redis"])
@pytest.mark.parametrize("case", list(TERMINAL))
def test_self_managed_terminal_failure_logs_record_before_complete(
    make: Any, mode: str, case: str
) -> None:
    """D6b / D13.1 (revised): the failure record precedes the delete."""
    h = make([_terminal(case)], mode=mode)
    assert h.run() == EXIT_OK
    assert _order(h) == ["line:failed_job", "complete", "line:job"]


@pytest.mark.parametrize("case", list(TERMINAL))
def test_managed_terminal_failure_completes_before_failed_job(make: Any, case: str) -> None:
    """Laravel order: complete, then failed_job, then failed (``Job::fail``)."""
    h = make([_terminal(case)], mode="managed")
    assert h.run() == EXIT_OK
    assert _order(h) == [
        "started",
        "line:failed_job",
        "complete",
        "failed_job",
        "failed",
        "line:job",
    ]


@pytest.mark.parametrize("mode", ["sqs", "redis", "managed"])
def test_failure_record_is_written_even_when_complete_raises(make: Any, mode: str) -> None:
    d = delivery("raise")
    h = make([d, delivery()], mode=mode, complete_error=AmbiguousAcknowledgementError("maybe"))
    assert h.run() == EXIT_FATAL
    assert [name for name in _order(h) if name != "started"] == ["line:failed_job", "complete"]
    assert h.lines()[0]["payload"] == d.body


# --- acknowledgement failures ------------------------------------------------------------------


def test_agent_4xx_is_logged_and_the_worker_continues(make: Any) -> None:
    first, second = delivery(), delivery()
    h = make(
        [first, second],
        mode="managed",
        agent=True,
        complete_error=AgentProtocolError("rejected", status=409),
    )
    assert h.run() == EXIT_OK
    assert h.env.of("complete") == [first.message_id, second.message_id]
    assert types_of(h) == ["started", "processed", "started", "processed"]


def test_agent_unavailable_on_report_emits_event_then_exits_0(make: Any) -> None:
    h = make(
        [delivery(), delivery()],
        mode="managed",
        agent=True,
        complete_error=AgentUnavailableError("agent 503"),
    )
    assert h.run() == EXIT_OK
    assert types_of(h) == ["started", "processed"]
    assert len(h.consumer.receives) == 1
    assert h.env.lifespan == ["enter", "exit"]


@pytest.mark.parametrize(
    "error",
    [
        AmbiguousAcknowledgementError("maybe"),
        BrokerConnectionError("gone"),
        LeaseLostError("lost"),
        TransportError("odd"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_fatal_acknowledgement_errors_stop_with_exit_1(make: Any, error: Exception) -> None:
    h = make([delivery(), delivery()], mode="managed", complete_error=error)
    assert h.run() == EXIT_FATAL
    assert types_of(h) == ["started"]
    assert len(h.consumer.receives) == 1


def test_release_ack_failure_is_never_followed_by_a_second_outcome(make: Any) -> None:
    h = make(
        [delivery("raise", tries=3)],
        mode="managed",
        agent=True,
        release_error=AgentProtocolError("rejected", status=404),
    )
    h.run()
    assert h.env.names().count("release") == 1
    assert h.env.of("complete") == []
    assert types_of(h) == ["started", "released"]


# --- receive errors -----------------------------------------------------------------------------


def test_transient_receive_error_pauses_one_second_and_retries(make: Any) -> None:
    h = make([TransportError("throttled"), delivery()], stop_when_empty=False, max_jobs=1)
    pauses = _record_pauses(h)
    assert h.run() == EXIT_OK
    assert pauses[0] == 1.0
    assert h.env.ran == ["succeed"]


@pytest.mark.parametrize(
    ("error", "code"),
    [(AgentUnavailableError("down"), EXIT_OK), (BrokerConnectionError("down"), EXIT_FATAL)],
    ids=["agent", "broker"],
)
def test_fatal_receive_errors(make: Any, error: Exception, code: int) -> None:
    h = make([error, delivery()])
    assert h.run() == code
    assert h.env.ran == []


def test_missing_queue_on_receive_is_a_configuration_error(make: Any) -> None:
    h = make([ManagedQueueNotFoundError("emails"), delivery()])
    assert h.run() == EXIT_CONFIG
    assert h.env.ran == []


# --- lease watchdog ---------------------------------------------------------------------------


def test_watchdog_renews_during_a_blocking_sync_job_and_stops_before_reporting(
    make: Any,
) -> None:
    d = delivery("sleep:0.8")
    h = make([d], renewal=True, lease_seconds=1)
    assert h.run() == EXIT_OK
    names = h.env.names()
    assert names.count("renew") >= 1
    assert h.env.of("renew")[0] == (d.message_id, 1)
    assert "renew" not in names[names.index("complete") :]
    assert h.registry.leases == [1]


def test_lost_lease_reports_nothing_and_exits_1(make: Any) -> None:
    h = make(
        [delivery("sleep:0.6"), delivery()],
        mode="managed",
        renewal=True,
        lease_seconds=1,
        renew_error=LeaseLostError("gone"),
    )
    assert h.run() == EXIT_FATAL
    assert h.env.of("complete") == []
    assert h.env.of("release") == []
    assert types_of(h) == ["started"]
    assert len(h.consumer.receives) == 1


def test_repeated_renewal_failures_lose_the_lease(make: Any) -> None:
    h = make(
        [delivery("sleep:1.3")],
        renewal=True,
        lease_seconds=1,
        renew_error=TransportError("timeout"),
    )
    assert h.run() == EXIT_FATAL
    assert h.env.names().count("renew") == 3


def test_no_watchdog_without_renewal_support(make: Any) -> None:
    h = make([delivery("sleep:0.5")], lease_seconds=1)
    h.run()
    assert "renew" not in h.env.names()


# --- timeouts (D2) ---------------------------------------------------------------------------


@pytest.mark.parametrize("do", ["sleep:5", "spin:5", "async-sleep:5"])
def test_retryable_timeout_emits_released_and_exits_124_without_transport_call(
    make: Any, do: str
) -> None:
    h = make([delivery(do, tries=3, timeout=0.2)], mode="managed")
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.of("complete") == [] and h.env.of("release") == []
    assert types_of(h) == ["started", "released"]
    assert h.job_lines()[0]["status"] == "released"
    assert 150 <= h.events()[1]["duration_ms"] < 2000
    assert h.telemetry.lock_timeouts[-1] == 0.5


def test_timeout_on_last_attempt_completes_then_failed_job_then_failed(make: Any) -> None:
    d = delivery("sleep:5", tries=2, attempt=2, timeout=0.2)
    h = make([d], mode="managed")
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.names() == ["event", "line", "complete", "event", "event", "line"]
    assert types_of(h) == ["started", "failed_job", "failed"]
    exception = h.events()[1]["exception"]
    assert isinstance(exception, JobTimeoutError)
    assert exception.__traceback__ is not None


def test_fail_on_timeout_is_terminal_with_attempts_left(make: Any) -> None:
    h = make([delivery("sleep:5", tries=4, timeout=0.2, fail_on_timeout=True)], mode="sqs")
    assert exit_code(h) == EXIT_TIMEOUT
    assert _order(h) == ["line:failed_job", "complete", "line:job"]
    record, job_line = h.lines()
    assert isinstance(record["exception"], JobTimeoutError)
    assert job_line["status"] == "failed"


@pytest.mark.parametrize("mode", ["sqs", "redis"])
def test_terminal_timeout_self_managed_record_survives_failed_complete(
    make: Any, mode: str
) -> None:
    h = make(
        [delivery("sleep:5", timeout=0.2)],
        mode=mode,
        complete_error=AmbiguousAcknowledgementError("maybe"),
    )
    assert exit_code(h) == EXIT_TIMEOUT
    assert _order(h) == ["line:failed_job", "complete", "line:job"]


def test_worker_default_timeout_applies_when_message_omits_it(make: Any) -> None:
    h = make([delivery("sleep:5", tries=3)], timeout=0.2)
    assert exit_code(h) == EXIT_TIMEOUT


def test_timer_is_disarmed_after_the_job(make: Any) -> None:
    timers: list[tuple[float, float]] = []

    def check() -> None:
        timers.append(signal.getitimer(signal.ITIMER_REAL))

    h = make([delivery(timeout=30), check])
    assert exit_code(h) == EXIT_OK
    assert timers == [(0.0, 0.0)]


def test_zero_timeout_disables_the_alarm(make: Any) -> None:
    h = make([delivery("sleep:0.3", timeout=0)], timeout=0.1)
    assert exit_code(h) == EXIT_OK


# --- signals ----------------------------------------------------------------------------------


def test_sigterm_mid_job_finishes_reports_and_stops(make: Any) -> None:
    first = delivery("sigterm")
    h = make([first, delivery()], mode="managed", stop_when_empty=False)
    assert h.run() == EXIT_OK
    assert h.env.of("complete") == [first.message_id]
    assert types_of(h) == ["started", "processed"]
    assert h.consumer.interrupted == 1
    assert len(h.consumer.receives) == 1
    assert h.env.lifespan == ["enter", "exit"]


def test_delivery_handed_over_after_the_signal_still_runs(make: Any) -> None:
    import os

    d = delivery()

    def receive_then_signal() -> Any:
        os.kill(os.getpid(), signal.SIGTERM)
        return d

    h = make([receive_then_signal, delivery()], stop_when_empty=False)
    assert h.run() == EXIT_OK
    assert h.env.of("complete") == [d.message_id]


def test_signal_while_sleeping_wakes_the_worker(make: Any) -> None:
    import os
    import threading
    import time

    h = make([None], stop_when_empty=False, sleep=30, queues=("a", "b"))
    timer = threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGTERM))
    timer.start()
    started = time.monotonic()
    assert h.run() == EXIT_OK
    assert time.monotonic() - started < 5


# --- loop and stop conditions ------------------------------------------------------------------


def _record_pauses(h: Harness) -> list[float]:
    pauses: list[float] = []

    async def pause(runtime: object, seconds: float) -> None:
        pauses.append(seconds)

    h.worker._pause = pause  # type: ignore[method-assign]
    return pauses


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_max_jobs(make: Any) -> None:
    h = make([delivery(), delivery(), delivery()], stop_when_empty=False, max_jobs=2)
    assert h.run() == EXIT_OK
    assert len(h.env.ran) == 2


def test_rest_between_jobs_and_sleep_after_empty_poll(make: Any) -> None:
    h = make(
        [delivery(), None, delivery()],
        queues=("high", "low"),
        rest=2.5,
        sleep=4,
        stop_when_empty=False,
        max_jobs=2,
    )
    pauses = _record_pauses(h)
    h.run()
    assert pauses == [2.5, 4]
    assert h.consumer.receives[0] == (("high", "low"), 0.0)


def test_max_time_checked_between_jobs(make: Any) -> None:
    clock = FakeClock()
    h = make([delivery("succeed"), delivery(), delivery()], stop_when_empty=False, max_time=10)
    h.worker._clock = clock
    pauses = _record_pauses(h)

    original = h.consumer.receive

    def receive(queues: Any, wait: float) -> Any:
        clock.now += 6
        return original(queues, wait)

    h.consumer.receive = receive  # type: ignore[method-assign]
    assert h.run() == EXIT_OK
    assert len(h.env.ran) == 2
    assert pauses == [0.0]


def test_stop_when_empty_for_counts_from_last_job(make: Any) -> None:
    clock = FakeClock()
    h = make([None, delivery(), None, None, None], stop_when_empty=False, stop_when_empty_for=5)
    h.worker._clock = clock
    original = h.consumer.receive

    def receive(queues: Any, wait: float) -> Any:
        clock.now += 2
        return original(queues, wait)

    h.consumer.receive = receive  # type: ignore[method-assign]
    _record_pauses(h)
    assert h.run() == EXIT_OK
    # start 1000; empty @1002; job @1004; empties @1006, @1008, @1010 (6 s after the job)
    assert len(h.consumer.receives) == 5


def test_stop_when_empty_stops_on_first_empty_poll(make: Any) -> None:
    h = make([None, delivery()])
    assert h.run() == EXIT_OK
    assert h.env.ran == []


# --- queues and receive waits -----------------------------------------------------------------


def test_single_sqs_queue_long_polls_without_sleep(make: Any) -> None:
    h = make([None], queues=("emails",))
    pauses = _record_pauses(h)
    h.run()
    assert h.consumer.receives == [(("emails",), 20.0)]
    assert pauses == []


def test_default_queue_from_config(make: Any) -> None:
    h = make([None])
    h.run()
    assert h.consumer.receives[0][0] == ("default",)


def test_redis_blocks_for_sleep_seconds(make: Any) -> None:
    h = make([None], mode="redis", sleep=2)
    h.run()
    assert h.consumer.receives == [(("default",), 2)]


def test_managed_without_agent_uses_the_assignment(make: Any) -> None:
    h = make([None], mode="managed")
    h.run()
    assert h.consumer.receives[0] == (("assigned",), 20.0)


def test_agent_mode_uses_assignment_and_sleeps_after_empty_poll(make: Any) -> None:
    """Laravel sleeps ``--sleep`` after an empty pop (``Queue/Worker.php:286-290``)."""
    h = make([None, delivery()], mode="managed", agent=True, stop_when_empty=False, max_jobs=1)
    pauses = _record_pauses(h)
    h.run()
    assert [queues for queues, _ in h.consumer.receives] == [("assigned",), ("assigned",)]
    assert pauses == [3.0]


def test_agent_mode_accepts_matching_queue(make: Any) -> None:
    h = make([None], mode="managed", agent=True, queues=("assigned",))
    assert h.run() == EXIT_OK


def test_agent_mode_queue_conflict_exits_2(make: Any, caplog: pytest.LogCaptureFixture) -> None:
    h = make([delivery()], mode="managed", agent=True, queues=("other",))
    assert h.run() == EXIT_CONFIG
    assert h.env.lifespan == []
    assert h.consumer.receives == []
    assert "assigned" in caplog.text and "other" in caplog.text


def test_configuration_error_at_startup_exits_2(
    make: Any, caplog: pytest.LogCaptureFixture
) -> None:
    h = make([delivery()], config=ConfigurationError("LARAVEL_CLOUD_QUEUES_BACKEND is not set."))
    assert h.run() == EXIT_CONFIG
    assert h.env.lifespan == []
    assert "LARAVEL_CLOUD_QUEUES_BACKEND is not set." in caplog.text


# --- per-job isolation --------------------------------------------------------------------------


def test_each_delivery_gets_its_own_context(make: Any) -> None:
    first, second = delivery(attempt=1, tries=3), delivery(attempt=2, tries=3, queue="other")
    h = make([first, second])
    h.run()
    a, b = h.env.contexts
    assert a is not b
    assert a.fields["message_id"] == first.message_id
    assert b.fields == {
        "job_name": "demo.job",
        "uuid": "uuid-1",
        "message_id": second.message_id,
        "queue": "other",
        "attempt": 2,
        "max_tries": 3,
    }


@pytest.mark.parametrize(
    "empty_options",
    [{"stop_when_empty": True}, {"stop_when_empty": False, "stop_when_empty_for": 0}],
)
def test_failed_receive_is_not_an_empty_poll(make: Any, empty_options: dict[str, Any]) -> None:
    h = make([TransportError("throttled"), delivery()], max_jobs=1, **empty_options)
    pauses = _record_pauses(h)
    assert h.run() == EXIT_OK
    assert h.env.ran == ["succeed"]
    assert pauses == [1.0]
    assert len(h.consumer.receives) == 2


@pytest.mark.parametrize("mode", ["sqs", "managed"])
@pytest.mark.parametrize("lost_during_stop", [False, True])
def test_timeout_never_reports_after_lease_loss(
    make: Any, monkeypatch: pytest.MonkeyPatch, mode: str, lost_during_stop: bool
) -> None:
    from laravel_cloud_queues.worker._watchdog import Watchdog

    if lost_during_stop:
        original = Watchdog.signal_stop

        def stop(watchdog: Watchdog) -> None:
            original(watchdog)
            watchdog.lost = True

        monkeypatch.setattr(Watchdog, "signal_stop", stop)
    h = make(
        [delivery("sleep:5", timeout=0.6)],
        mode=mode,
        renewal=True,
        lease_seconds=1,
        renew_error=None if lost_during_stop else LeaseLostError("gone"),
    )
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.of("complete") == []
    assert h.env.of("release") == []
    assert h.lines() == []
    assert types_of(h) == (["started"] if mode == "managed" else [])


# --- process edges ----------------------------------------------------------------------------


def test_configuration_error_after_start_exits_2_and_closes_the_consumer(make: Any) -> None:
    from contextlib import asynccontextmanager

    h = make([delivery()])

    class Target:
        registry = h.registry

        @asynccontextmanager
        async def lifespan(self) -> Any:
            raise ConfigurationError("lifespan needs a setting")
            yield

    h.worker._target = Target()  # type: ignore[assignment]
    assert h.run() == EXIT_CONFIG
    assert h.consumer.closed
    assert h.env.of("complete") == []


def test_consumer_close_failure_is_logged_not_raised(
    make: Any, caplog: pytest.LogCaptureFixture
) -> None:
    h = make([delivery()])

    def close() -> None:
        raise OSError("socket already gone")

    h.consumer.close = close  # type: ignore[method-assign]
    assert h.run() == EXIT_OK
    assert "Closing the consumer failed (OSError)" in caplog.text


def test_alarm_after_the_job_finished_is_ignored(make: Any) -> None:
    h = make([])
    h.worker._on_alarm(signal.SIGALRM, None)  # no running job: must not exit the process


def test_timeout_with_a_healthy_watchdog_still_reports(make: Any) -> None:
    d = delivery("sleep:5", timeout=0.3)
    h = make([d], renewal=True, lease_seconds=30)
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.of("complete") == [d.message_id]


# --- async consumer path ------------------------------------------------------------------------


def _forbid_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_thread(*args: object, **kwargs: object) -> None:
        raise AssertionError("the worker loop must not hop to a thread")

    monkeypatch.setattr(worker_module.anyio.to_thread, "run_sync", no_thread)


def test_native_async_consumer_is_awaited_on_the_loop(
    make: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    first, second = delivery(), delivery("raise", tries=3, backoff=[4])
    h = make([first, second], native=True, lease_seconds=7)
    _forbid_threads(monkeypatch)
    assert h.run() == EXIT_OK
    assert h.native is not None
    assert h.native.calls == [
        ("receive", True),
        ("complete", True),
        ("receive", True),
        ("release", True),
        ("receive", True),
    ]
    assert h.env.of("complete") == [first.message_id]
    assert h.env.of("release") == [(second.message_id, 4)]
    assert h.native.closed
    assert not h.consumer.closed  # aclose belongs to the async consumer
    assert h.registry.leases == [7]


def test_lifecycle_events_leave_the_loop(make: Any) -> None:
    h = make([delivery("raise")], mode="managed")
    assert h.run() == EXIT_OK
    assert types_of(h) == ["started", "failed_job", "failed"]
    assert h.telemetry.async_emits == 1  # "started"
    assert h.telemetry.emit_on_main[1:] == [False, False]  # the reporting records


def test_timeout_events_are_emitted_synchronously_from_sigalrm(make: Any) -> None:
    h = make([delivery("sleep:5", tries=3, timeout=0.2)], mode="managed")
    assert exit_code(h) == EXIT_TIMEOUT
    assert types_of(h) == ["started", "released"]
    assert h.telemetry.async_emits == 1  # only "started"


def test_stop_signal_interrupts_the_async_consumer(make: Any) -> None:
    h = make([delivery("sigterm"), delivery()], native=True, stop_when_empty=False)
    assert h.run() == EXIT_OK
    assert h.native is not None
    assert h.native.interrupted == 1


def test_async_handler_renews_through_the_async_consumer_on_the_loop(make: Any) -> None:
    d = delivery("async-sleep:0.8")
    h = make([d], native=True, renewal=True, lease_seconds=1)
    assert h.run() == EXIT_OK
    assert h.native is not None
    renewals = [call for call in h.native.calls if call[0] == "renew"]
    assert renewals and all(on_main for _, on_main in renewals)
    assert h.env.of("renew")[0] == (d.message_id, 1)
    names = h.env.names()
    assert "renew" not in names[names.index("complete") :]


def test_async_handler_lost_lease_reports_nothing(make: Any) -> None:
    h = make(
        [delivery("async-sleep:0.6")],
        native=True,
        renewal=True,
        lease_seconds=1,
        renew_error=LeaseLostError("gone"),
    )
    assert h.run() == EXIT_FATAL
    assert h.env.of("complete") == []


def test_sync_handler_renews_through_the_blocking_twin_off_the_loop(make: Any) -> None:
    h = make([delivery("sleep:0.8")], native=True, renewal=True, lease_seconds=1)
    assert h.run() == EXIT_OK
    assert h.native is not None
    assert "renew" in h.env.names()
    assert [name for name, _ in h.native.calls if name == "renew"] == []


def test_async_handler_timeout_completes_through_the_blocking_twin(make: Any) -> None:
    d = delivery("async-sleep:5", timeout=0.3)
    h = make([d], native=True, renewal=True, lease_seconds=30)
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.of("complete") == [d.message_id]
    assert h.native is not None
    assert "complete" not in [name for name, _ in h.native.calls]


def test_timeout_flushes_logging_before_exiting(make: Any) -> None:
    h = make([delivery("sleep:5", tries=3, timeout=0.2)])
    assert exit_code(h) == EXIT_TIMEOUT
    assert h.env.flushes == [0.5]
    assert h.env.exits == [EXIT_TIMEOUT]


def test_timeout_exits_even_when_the_flush_raises(
    make: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(timeout: float = 1.0) -> bool:
        raise RuntimeError("flush broke")

    h = make([delivery("sleep:5", tries=3, timeout=0.2)])
    monkeypatch.setattr(worker_module, "flush", broken)
    with pytest.raises(BaseException) as raised:
        h.run()
    assert h.env.exits == [EXIT_TIMEOUT]
    assert _find(raised.value) is not None


def test_cancelled_worker_still_closes_the_consumer(make: Any) -> None:
    h = make([delivery(), delivery()], native=True, stop_when_empty=False)
    assert h.native is not None

    async def main() -> None:
        with worker_module.anyio.CancelScope() as scope:
            h.native.on_receive = scope.cancel  # type: ignore[union-attr]
            await h.worker._main()

    worker_module.anyio.run(main, backend="asyncio")
    assert h.native.closed
