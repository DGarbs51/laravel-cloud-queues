"""Retry policy math and delay normalization (PROJECT_SCOPE.md §10, §12, D4)."""

from __future__ import annotations

import math
from datetime import timedelta

import pytest

from laravel_cloud_queues.errors import ConfigurationError, InvalidQueueOptionError
from laravel_cloud_queues.jobs.policy import (
    MAX_JOB_TIMEOUT,
    ResolvedPolicy,
    RetryPolicy,
    WorkerDefaults,
    normalize_delay,
    normalize_release_delay,
)


def resolved(tries: int = 1, backoff: tuple[float, ...] = (0,)) -> ResolvedPolicy:
    return ResolvedPolicy(tries=tries, backoff=backoff, timeout=60.0, fail_on_timeout=False)


def test_worker_defaults_match_laravel() -> None:
    """Default tries 1, backoff 0, timeout 60 (§12; Laravel WorkerOptions)."""
    policy = RetryPolicy().resolve(WorkerDefaults())
    assert policy == ResolvedPolicy(tries=1, backoff=(0,), timeout=60.0, fail_on_timeout=False)


def test_declared_fields_win_over_worker_defaults() -> None:
    defaults = WorkerDefaults(tries=7, backoff=(9,), timeout=5, fail_on_timeout=True)
    policy = RetryPolicy(tries=3, backoff=[1, 5]).resolve(defaults)
    assert policy == ResolvedPolicy(tries=3, backoff=(1, 5), timeout=5, fail_on_timeout=True)
    assert RetryPolicy(backoff=2.5).resolve(defaults).backoff == (2.5,)
    assert RetryPolicy(timeout=0, fail_on_timeout=False).resolve(defaults).timeout == 0


def test_backoff_list_is_frozen_to_a_tuple() -> None:
    backoff = [1, 2]
    policy = RetryPolicy(backoff=backoff)
    backoff.append(3)
    assert policy.backoff == (1, 2)
    assert hash(policy) == hash(RetryPolicy(backoff=(1, 2)))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tries": -1},
        {"tries": True},
        {"tries": 1.5},
        {"backoff": -1},
        {"backoff": [1, -1]},
        {"backoff": [math.nan]},
        {"backoff": math.inf},
        {"backoff": []},
        {"backoff": "1,5"},
        {"backoff": [True]},
        {"timeout": -0.1},
        {"timeout": math.inf},
        {"timeout": 604_801},
        {"fail_on_timeout": 1},
    ],
)
def test_invalid_policy_is_a_configuration_error(kwargs: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_timeout_bounds() -> None:
    """0 disables; at most 7 days (setitimer overflows on huge values)."""
    assert MAX_JOB_TIMEOUT == 604_800
    assert RetryPolicy(timeout=0).timeout == 0
    assert RetryPolicy(timeout=MAX_JOB_TIMEOUT).timeout == MAX_JOB_TIMEOUT
    assert WorkerDefaults(timeout=MAX_JOB_TIMEOUT).timeout == MAX_JOB_TIMEOUT
    for invalid in (MAX_JOB_TIMEOUT + 0.5, -1, math.nan):
        with pytest.raises(ConfigurationError):
            WorkerDefaults(timeout=invalid)


@pytest.mark.parametrize(
    ("tries", "attempt", "exceeded", "last"),
    [
        (1, 1, False, True),
        (1, 2, True, True),
        (3, 1, False, False),
        (3, 2, False, False),
        (3, 3, False, True),
        (3, 4, True, True),
        (0, 1, False, False),
        (0, 10_000, False, False),
    ],
)
def test_attempt_checks(tries: int, attempt: int, exceeded: bool, last: bool) -> None:
    """Pre-run ``tries > 0 and attempt > tries``; terminal after an exception
    ``tries > 0 and attempt >= tries``; ``tries=0`` unlimited.

    Laravel: framework/src/Illuminate/Queue/Worker.php:711 (pre-run) and :737 (after
    exception)."""
    policy = resolved(tries=tries)
    assert policy.exceeded_before_run(attempt) is exceeded
    assert policy.is_last_attempt(attempt) is last


@pytest.mark.parametrize(
    ("backoff", "attempt", "delay"),
    [
        ((1, 5, 30), 1, 1),
        ((1, 5, 30), 2, 5),
        ((1, 5, 30), 3, 30),
        ((1, 5, 30), 4, 30),
        ((1, 5, 30), 99, 30),
        ((10,), 1, 10),
        ((10,), 5, 10),
        ((0,), 3, 0),
        ((0.2,), 1, 1),
        ((1.5, 0), 1, 2),
        ((1.5, 0), 2, 0),
        ((50_000,), 1, 43_200),
        ((), 1, 0),
    ],
)
def test_retry_delay(backoff: tuple[float, ...], attempt: int, delay: int) -> None:
    """``backoff[attempt - 1]``, last value repeats (Laravel Worker.php:826); positive
    fractions round up (Symfony; labeled deviation: Laravel truncates with ``(int)``);
    clamped to 43,200."""
    assert resolved(backoff=backoff).retry_delay(attempt) == delay


@pytest.mark.parametrize(
    ("delay", "seconds"),
    [
        (0, 0),
        (0.0, 0),
        (0.001, 1),
        (1, 1),
        (29.2, 30),
        (900, 900),
        (timedelta(seconds=1.5), 2),
        (timedelta(minutes=15), 900),
    ],
)
def test_normalize_delay(delay: float | timedelta, seconds: int) -> None:
    assert normalize_delay(delay) == seconds


@pytest.mark.parametrize(
    "delay",
    [-1, -0.001, timedelta(seconds=-1), math.nan, math.inf, 900.5, 901, "5", True, None],
)
def test_normalize_delay_rejects(delay: object) -> None:
    with pytest.raises(InvalidQueueOptionError):
        normalize_delay(delay)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("delay", "seconds"),
    [(0, 0), (0.1, 1), (3600, 3600), (timedelta(hours=13), 43_200), (10**9, 43_200)],
)
def test_normalize_release_delay(delay: float | timedelta, seconds: int) -> None:
    assert normalize_release_delay(delay) == seconds


@pytest.mark.parametrize("delay", [-1, math.nan, -math.inf, math.inf])
def test_normalize_release_delay_rejects(delay: float) -> None:
    with pytest.raises(InvalidQueueOptionError):
        normalize_release_delay(delay)
