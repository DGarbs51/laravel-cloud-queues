"""Alarm diagnostics bypass application logging, even in telemetry failure paths."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from laravel_cloud_queues.observability import _guard


def test_signal_safe_diagnostics_restore_normal_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    logger, write = Mock(), Mock(side_effect=lambda _, data: len(data))
    monkeypatch.setattr(_guard, "_logger", logger)
    monkeypatch.setattr(_guard.os, "write", write)
    with _guard.signal_safe():
        with _guard.signal_safe():
            _guard.log_failure("socket failed")
        _guard.log_failure("stdout failed")
    assert write.call_args_list == [((2, b"socket failed\n"),), ((2, b"stdout failed\n"),)]
    logger.warning.assert_not_called()
    _guard.log_failure("normal diagnostic")
    logger.warning.assert_called_once_with("%s", "normal diagnostic")


def test_raw_diagnostic_ignores_write_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    write = Mock(side_effect=OSError("stderr unavailable"))
    monkeypatch.setattr(_guard.os, "write", write)
    _guard.raw_diagnostic("timeout")
    write.assert_called_once_with(2, b"timeout\n")


@pytest.mark.parametrize("blocking", [False, True])
def test_raw_write_restores_descriptor_flags_on_failure(
    monkeypatch: pytest.MonkeyPatch, blocking: bool
) -> None:
    flags = Mock()
    monkeypatch.setattr(_guard.os, "get_blocking", lambda _: blocking)
    monkeypatch.setattr(_guard.os, "set_blocking", flags)
    monkeypatch.setattr(_guard.os, "write", Mock(side_effect=BlockingIOError("pipe full")))
    _guard.raw_write(2, b"timeout\n")
    assert flags.call_args_list == ([((2, False),), ((2, True),)] if blocking else [])


def test_raw_write_drains_a_large_record_without_truncating() -> None:
    import os
    import threading

    read_fd, write_fd = os.pipe()
    chunks: list[bytes] = []
    data = b"x" * 200_000 + b"\n"

    def read() -> None:
        while chunk := os.read(read_fd, 4096):
            chunks.append(chunk)

    reader = threading.Thread(target=read)
    reader.start()
    try:
        _guard.raw_write(write_fd, data, timeout=2)
        assert os.get_blocking(write_fd)
    finally:
        os.close(write_fd)
        reader.join(3)
        os.close(read_fd)
    assert not reader.is_alive()
    assert b"".join(chunks) == data


def test_raw_write_with_no_data_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    write = Mock()
    monkeypatch.setattr(_guard.os, "write", write)
    _guard.raw_write(2, b"", timeout=1)
    write.assert_not_called()


def test_raw_write_gives_up_when_the_descriptor_stays_full() -> None:
    import os
    import time

    read_fd, write_fd = os.pipe()
    try:
        started = time.monotonic()
        _guard.raw_write(write_fd, b"x" * 1_000_000, timeout=0.05)
        assert time.monotonic() - started < 1
        assert os.get_blocking(write_fd)
        os.set_blocking(read_fd, False)
        assert 0 < len(os.read(read_fd, 2_000_000)) < 1_000_000
    finally:
        os.close(write_fd)
        os.close(read_fd)


def test_log_failure_ignores_reentry_and_logger_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    import logging

    logger = logging.getLogger("laravel_cloud_queues.observability")
    messages: list[str] = []

    class Handler(logging.Handler):  # skipcq: PY-A6006 - test-only capture handler
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())
            _guard.log_failure("nested")

    handler = Handler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        _guard.log_failure("outer")
    finally:
        logger.removeHandler(handler)
    assert messages == ["outer"]

    monkeypatch.setattr(_guard, "_logger", Mock(warning=Mock(side_effect=RuntimeError("down"))))
    _guard.log_failure("still swallowed")
    assert getattr(_guard._local, "in_log", False) is False
