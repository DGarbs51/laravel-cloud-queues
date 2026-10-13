"""Prove memory recycling preserves delivery and reporting ownership."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import laravel_cloud_queues.worker as worker_module
from laravel_cloud_queues.errors import (
    AgentUnavailableError,
    BrokerConnectionError,
    ConfigurationError,
)
from laravel_cloud_queues.worker import _memory
from laravel_cloud_queues.worker._memory import _cgroup_limit as _read_cgroup_limit
from tests.unit.worker.doubles import delivery


@pytest.fixture(autouse=True)
def memory_environment(monkeypatch):
    monkeypatch.delenv("LARAVEL_CLOUD_QUEUES_MEMORY", raising=False)
    monkeypatch.setattr(_memory, "_cgroup_limit", lambda: 100 * 1024**2)


def test_memory_limit_stops_after_reporting(make, monkeypatch, caplog):
    harness = make([delivery(), delivery()], mode="managed", memory="64", native=True)

    def rss():
        assert harness.env.names() == ["event", "complete", "event", "line"]
        assert harness.env.ran == ["succeed"]
        return 65 * 1024**2

    monkeypatch.setattr(worker_module, "_current_rss", rss)
    with caplog.at_level(logging.INFO):
        assert harness.run() == 12
    assert len(harness.consumer.receives) == 1
    assert harness.native.closed
    assert harness.env.lifespan == ["enter", "exit"]
    assert "memory limit" in caplog.text


def test_no_memory_limit_outside_managed_mode(make, monkeypatch):
    rss = Mock(side_effect=AssertionError("disabled memory checks must not measure RSS"))
    monkeypatch.delenv("LARAVEL_CLOUD_QUEUES_MEMORY", raising=False)
    monkeypatch.setattr(worker_module, "_current_rss", rss)
    harness = make([delivery(), delivery()], mode="sqs")
    assert harness.run() == 0
    assert len(harness.env.ran) == 2
    rss.assert_not_called()


@pytest.mark.parametrize("mode", ["sqs", "redis", "managed"])
@pytest.mark.parametrize("action", ["succeed", "release:5", "raise", "fail", "defect"])
@pytest.mark.parametrize("native", [False, True])
def test_memory_recycling_preserves_each_reported_outcome(make, monkeypatch, mode, action, native):
    harness = make([delivery(action), delivery()], mode=mode, native=native, memory="64")

    def rss():
        assert harness.job_lines()
        assert harness.env.of("release" if action == "release:5" else "complete")
        return 64 * 1024**2

    monkeypatch.setattr(worker_module, "_current_rss", rss)
    assert harness.run() == worker_module.EXIT_MEMORY_LIMIT
    assert len(harness.consumer.receives) == 1


@pytest.mark.parametrize("rss", [0, 63 * 1024**2])
def test_memory_below_limit_continues_fetching(make, monkeypatch, rss):
    monkeypatch.setattr(worker_module, "_current_rss", lambda: rss)
    harness = make([delivery(), delivery()], memory="64")
    assert harness.run() == 0
    assert len(harness.env.ran) == 2


@pytest.mark.parametrize("mode", ["sqs", "redis", "managed"])
@pytest.mark.parametrize("value", ["0", "0%"])
def test_zero_disables_memory_recycling(make, monkeypatch, mode, value):
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_MEMORY", "1")
    monkeypatch.setattr(worker_module, "_current_rss", Mock(side_effect=AssertionError))
    assert make([delivery()], mode=mode, memory=value).run() == 0


@pytest.mark.parametrize(
    ("error", "code"),
    [(BrokerConnectionError("ambiguous"), 1), (AgentUnavailableError("lost agent"), 0)],
)
def test_reporting_failure_keeps_its_exit_code(make, monkeypatch, error, code):
    rss = Mock(side_effect=AssertionError("fatal reporting must keep its exit code"))
    monkeypatch.setattr(worker_module, "_current_rss", rss)
    harness = make([delivery()], complete_error=error, memory="1")
    assert harness.run() == code
    rss.assert_not_called()


def test_empty_poll_does_not_check_memory(make, monkeypatch):
    rss = Mock(side_effect=AssertionError("only reported deliveries trigger memory checks"))
    monkeypatch.setattr(worker_module, "_current_rss", rss)
    assert make(memory="1").run() == 0
    rss.assert_not_called()


@pytest.mark.parametrize(
    ("option", "env", "managed", "expected_mib"),
    [
        ("32", "invalid", True, 32),
        ("75%", "64", True, 75),
        (None, "64", True, 64),
        (None, "50%", False, 50),
        (None, None, True, 90),
        (None, None, False, None),
        ("12.5", None, False, 12.5),
        (" 12.5% ", None, True, 12.5),
        ("100%", None, True, 100),
        ("0.00000001", None, False, 1 / 1024**2),
    ],
)
def test_memory_option_environment_default_precedence(
    monkeypatch, option, env, managed, expected_mib
):
    if env is not None:
        monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_MEMORY", env)
    assert _memory.resolve_limit(option, managed=managed) == (
        None if expected_mib is None else int(expected_mib * 1024**2)
    )


def test_managed_default_recycles_without_flag(make, monkeypatch, caplog):
    monkeypatch.setattr(worker_module, "_current_rss", lambda: 91 * 1024**2)
    with caplog.at_level(logging.INFO):
        assert make([delivery(), delivery()], mode="managed").run() == 12
    assert "Worker memory limit: 90.00 MiB RSS" in caplog.text


def test_environment_recycles_without_flag(make, monkeypatch):
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_MEMORY", "64")
    monkeypatch.setattr(worker_module, "_current_rss", lambda: 65 * 1024**2)
    assert make([delivery()], mode="sqs").run() == 12


@pytest.mark.parametrize("value", ["", "no", "-1", "nan", "inf", "101%", "-1%", "1e308"])
def test_invalid_memory_is_configuration_error(value):
    with pytest.raises(ConfigurationError, match="Memory"):
        _memory.resolve_limit(value, managed=False)


def test_invalid_environment_fails_before_opening_consumer(make, monkeypatch, caplog):
    monkeypatch.setenv("LARAVEL_CLOUD_QUEUES_MEMORY", "bad")
    harness = make([delivery()])
    assert harness.run() == 2
    assert harness.registry.leases == []
    assert harness.consumer.receives == []
    assert "Memory" in caplog.text


def test_percentage_requires_finite_cgroup_limit(monkeypatch):
    monkeypatch.setattr(_memory, "_cgroup_limit", lambda: None)
    with pytest.raises(ConfigurationError, match="finite cgroup"):
        _memory.resolve_limit("90%", managed=False)
    assert _memory.resolve_limit(None, managed=True) is None
    assert _memory.resolve_limit("64", managed=True) == 64 * 1024**2
    assert _memory.resolve_limit("0%", managed=True) is None


@pytest.mark.parametrize(
    ("content", "expected"),
    [("104857600\n", 104857600), ("max\n", None), ("bad", None), ("0", None), ("-1", None)],
)
def test_read_cgroup_limit(monkeypatch, content, expected):
    monkeypatch.setattr(_memory, "_cgroup_limit", _read_cgroup_limit)

    def read(path, *, encoding):
        assert path == Path("/sys/fs/cgroup/memory.max")
        assert encoding == "ascii"
        return content

    monkeypatch.setattr(Path, "read_text", read)
    assert _memory._cgroup_limit() == expected


@pytest.mark.parametrize("error", [FileNotFoundError(), PermissionError(), UnicodeError()])
def test_unavailable_cgroup_has_no_default(monkeypatch, error):
    monkeypatch.setattr(_memory, "_cgroup_limit", _read_cgroup_limit)
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=error))
    assert _memory.resolve_limit(None, managed=True) is None


def test_linux_reads_current_rss_not_peak(monkeypatch):
    monkeypatch.setattr(_memory.sys, "platform", "linux")

    def read(path, *, encoding):
        assert path == Path("/proc/self/statm")
        assert encoding == "ascii"
        return "100000 123 0 0 0 0 0\n"

    monkeypatch.setattr(Path, "read_text", read)
    monkeypatch.setattr(_memory.os, "sysconf", lambda name: 4096)
    monkeypatch.setattr(_memory.resource, "getrusage", Mock(side_effect=AssertionError))
    assert _memory.current_rss() == 123 * 4096


@pytest.mark.parametrize("platform", ["darwin", "freebsd"])
def test_non_linux_peak_rss_units(monkeypatch, platform):
    monkeypatch.setattr(_memory.sys, "platform", platform)
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=AssertionError))
    monkeypatch.setattr(_memory.resource, "getrusage", lambda who: SimpleNamespace(ru_maxrss=123))
    assert _memory.current_rss() == (123 if platform == "darwin" else 123 * 1024)


@pytest.mark.parametrize("content", ["", "bad bad", "123"])
def test_malformed_statm_falls_back_to_peak(monkeypatch, content):
    monkeypatch.setattr(_memory.sys, "platform", "linux")
    monkeypatch.setattr(Path, "read_text", lambda path, **kwargs: content)
    monkeypatch.setattr(_memory.resource, "getrusage", lambda who: SimpleNamespace(ru_maxrss=123))
    assert _memory.current_rss() == 123 * 1024


@pytest.mark.parametrize("error", [FileNotFoundError(), PermissionError(), UnicodeError()])
def test_unavailable_statm_falls_back_to_peak(monkeypatch, error):
    monkeypatch.setattr(_memory.sys, "platform", "linux")
    monkeypatch.setattr(Path, "read_text", Mock(side_effect=error))
    monkeypatch.setattr(_memory.resource, "getrusage", lambda who: SimpleNamespace(ru_maxrss=123))
    assert _memory.current_rss() == 123 * 1024
