"""Socket writer failure paths that need no listener: peer checks, close, encoding."""

from __future__ import annotations

import socket
from unittest.mock import Mock

import pytest

from laravel_cloud_queues.observability import SocketEventSink, _guard
from laravel_cloud_queues.observability._socket import _peer_closed

_LOGGER = "laravel_cloud_queues.observability"


def _mock_socket() -> Mock:
    sock = Mock(spec=socket.socket)
    sock.recv.side_effect = BlockingIOError()
    return sock


def test_peer_closed_treats_socket_errors_as_closed() -> None:
    healthy = _mock_socket()
    assert _peer_closed(healthy) is False
    healthy.settimeout.assert_called_once_with(2.0)

    broken = _mock_socket()
    broken.recv.side_effect = OSError("reset")
    assert _peer_closed(broken) is True

    unusable = _mock_socket()
    unusable.setblocking.side_effect = OSError("bad descriptor")
    unusable.settimeout.side_effect = OSError("bad descriptor")
    assert _peer_closed(unusable) is True


def test_emit_swallows_unexpected_errors(caplog: pytest.LogCaptureFixture) -> None:
    sink = SocketEventSink("/nonexistent/lcq.sock")
    sock = _mock_socket()
    sock.send.side_effect = RuntimeError("not an OSError")
    sink._sock = sock
    with caplog.at_level("WARNING", logger=_LOGGER):
        assert sink.emit({"n": 1}) is False
    assert "observability socket emit failed" in caplog.text


def test_emit_fails_before_connecting_when_the_event_cannot_be_encoded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sink = SocketEventSink("/nonexistent/lcq.sock")
    with caplog.at_level("WARNING", logger=_LOGGER):
        assert sink.emit({"bad": object()}) is False
    assert "observability event encoding failed" in caplog.text
    assert sink._sock is None


def test_close_without_a_connection_is_a_noop() -> None:
    sink = SocketEventSink("/nonexistent/lcq.sock")
    sink.close()
    assert sink._sock is None


def test_close_swallows_socket_errors(caplog: pytest.LogCaptureFixture) -> None:
    sink = SocketEventSink("/nonexistent/lcq.sock")
    sock = _mock_socket()
    sock.close.side_effect = OSError("already closed")
    sink._sock = sock
    sink.close()
    assert sink._sock is None
    sock.close.assert_called_once()

    failing = _mock_socket()
    failing.close.side_effect = RuntimeError("not an OSError")
    sink._sock = failing
    with caplog.at_level("WARNING", logger=_LOGGER):
        sink.close()
    assert "observability socket close failed" in caplog.text
    assert sink._sock is None


def test_close_is_ignored_while_reentered() -> None:
    sink = SocketEventSink("/nonexistent/lcq.sock")
    sock = _mock_socket()
    sink._sock = sock
    assert _guard.begin_call()
    try:
        sink.close()
    finally:
        _guard.end_call()
    sock.close.assert_not_called()
    assert sink._sock is sock
