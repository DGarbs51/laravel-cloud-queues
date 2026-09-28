"""Unix socket writer: reuse, EOF reconnect, partial writes, lock timeout.

Conformance: ``src/Illuminate/Foundation/Cloud/Events.php:77-107`` (write loop,
5 zero-byte writes), ``:148-160`` (2 s connect and write timeouts), ``:180`` (EOF).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import suppress

import pytest
from typing_extensions import Self

from laravel_cloud_queues.observability import SocketEventSink, encode_event_line

pytestmark = pytest.mark.socket


class LineCollector:
    """Minimal NDJSON collector. The shared harness collector is owned by another lane."""

    def __init__(self, path: str | None = None, *, hold_reads: bool = False) -> None:
        self._owned: tempfile.TemporaryDirectory[str] | None = None
        if path is None:
            self._owned = tempfile.TemporaryDirectory(prefix="lcq", dir="/tmp")
            path = os.path.join(self._owned.name, "s.sock")
        self.path = path
        self._hold = threading.Event()
        if hold_reads:
            self._hold.set()
        self._lines: list[bytes] = []
        self._lines_lock = threading.Lock()
        self._accept_lock = threading.Lock()
        self._clients: list[socket.socket] = []
        self.accepts = 0
        self._stop = threading.Event()
        self._server: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._start()

    def _start(self) -> None:
        self._stop.clear()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.path)
        server.listen(16)
        server.settimeout(0.05)
        self._server = server
        thread = threading.Thread(target=self._accept_loop, name="lcq-obs-accept", daemon=True)
        self._thread = thread
        thread.start()

    def _accept_loop(self) -> None:
        server = self._server
        if server is None:
            return
        while not self._stop.is_set():
            try:
                conn, _addr = server.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                continue
            with self._accept_lock:
                self.accepts += 1
                self._clients.append(conn)
            threading.Thread(target=self._read_loop, args=(conn,), daemon=True).start()

    def _read_loop(self, conn: socket.socket) -> None:
        conn.settimeout(0.05)
        buf = bytearray()
        try:
            while not self._stop.is_set():
                if self._hold.is_set():
                    time.sleep(0.01)
                    continue
                try:
                    chunk = conn.recv(4096)
                except TimeoutError:
                    continue
                except OSError:
                    return
                if chunk == b"":
                    return
                buf.extend(chunk)
                while True:
                    newline = buf.find(b"\n")
                    if newline < 0:
                        break
                    line = bytes(buf[:newline])
                    del buf[: newline + 1]
                    with self._lines_lock:
                        self._lines.append(line)
        finally:
            with suppress(OSError):
                conn.close()

    def lines(self) -> list[bytes]:
        with self._lines_lock:
            return list(self._lines)

    def wait_for_lines(self, count: int, timeout: float = 2.0) -> list[bytes]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            found = self.lines()
            if len(found) >= count:
                return found
            time.sleep(0.01)
        return self.lines()

    def close_clients(self) -> None:
        with self._accept_lock:
            clients = list(self._clients)
        for conn in clients:
            with suppress(OSError):
                conn.shutdown(socket.SHUT_RDWR)
            with suppress(OSError):
                conn.close()

    def _shutdown_server(self) -> None:
        self._stop.set()
        self.close_clients()
        server = self._server
        self._server = None
        if server is not None:
            with suppress(OSError):
                server.close()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1)
            self._thread = None

    def restart(self) -> None:
        self._shutdown_server()
        if os.path.exists(self.path):
            os.unlink(self.path)
        self._start()

    def close(self) -> None:
        self._shutdown_server()
        owned = self._owned
        self._owned = None
        if owned is not None:
            owned.cleanup()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


@pytest.fixture
def collector() -> Iterator[LineCollector]:
    with LineCollector() as active:
        yield active


def test_reuses_one_connection(collector: LineCollector) -> None:
    sink = SocketEventSink(collector.path)
    assert sink.emit({"n": 1}) is True
    assert sink.emit({"n": 2}) is True
    lines = collector.wait_for_lines(2)
    assert [json.loads(line)["n"] for line in lines] == [1, 2]
    assert collector.accepts == 1
    sink.close()


def test_accepts_unix_scheme_and_bare_path(collector: LineCollector) -> None:
    sink = SocketEventSink("unix://" + collector.path)
    assert sink.emit({"path": "a/b"}) is True
    line = collector.wait_for_lines(1)[0]
    assert json.loads(line) == {"path": "a/b"}
    assert b"\\/" not in line


def test_reconnects_after_peer_close(collector: LineCollector) -> None:
    sink = SocketEventSink(collector.path)
    assert sink.emit({"n": 1}) is True
    assert len(collector.wait_for_lines(1)) == 1
    collector.close_clients()
    assert sink.emit({"n": 2}) is True
    lines = collector.wait_for_lines(2)
    assert [json.loads(line)["n"] for line in lines] == [1, 2]
    assert collector.accepts == 2


def test_outage_returns_false_quickly_and_recovers_after_restart() -> None:
    directory = tempfile.TemporaryDirectory(prefix="lcq", dir="/tmp")
    path = os.path.join(directory.name, "s.sock")
    try:
        sink = SocketEventSink(path)
        started = time.monotonic()
        assert sink.emit({"n": 1}) is False
        assert time.monotonic() - started < 0.5
        with LineCollector(path) as collector:
            assert sink.emit({"n": 2}) is True
            assert len(collector.wait_for_lines(1)) == 1
            collector.restart()
            assert sink.emit({"n": 3}) is True
            lines = collector.wait_for_lines(2)
        assert [json.loads(line)["n"] for line in lines] == [2, 3]
    finally:
        directory.cleanup()


def test_partial_writes_are_reassembled(
    collector: LineCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each ``send`` is capped at one byte on the real socket, so the write loop must iterate."""

    original = socket.socket.send
    calls = {"n": 0}

    def send(self: socket.socket, data: bytes | bytearray | memoryview, flags: int = 0) -> int:
        calls["n"] += 1
        view = memoryview(data)
        return original(self, view[:1], flags)

    monkeypatch.setattr(socket.socket, "send", send)
    event = {"pad": "partial-write-" * 4}
    sink = SocketEventSink(collector.path)
    assert sink.emit(event) is True
    expected = encode_event_line(event)
    assert calls["n"] == len(expected)
    lines = collector.wait_for_lines(1)
    assert lines == [expected[:-1]]


def test_five_consecutive_zero_byte_writes_disconnect(
    collector: LineCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}

    def send(self: socket.socket, data: bytes | bytearray | memoryview, flags: int = 0) -> int:
        calls["n"] += 1
        return 0

    monkeypatch.setattr(socket.socket, "send", send)
    sink = SocketEventSink(collector.path)
    assert sink.emit({"n": 1}) is False
    assert calls["n"] == 5
    monkeypatch.undo()
    assert sink.emit({"n": 1}) is True
    assert json.loads(collector.wait_for_lines(1)[0]) == {"n": 1}
    assert collector.accepts == 2


def test_zero_byte_counter_resets_after_progress(
    collector: LineCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = socket.socket.send
    phase = {"n": 0}

    def send(self: socket.socket, data: bytes | bytearray | memoryview, flags: int = 0) -> int:
        phase["n"] += 1
        count = phase["n"]
        if count <= 4 or 6 <= count <= 9:
            return 0
        view = memoryview(data)
        if count == 5:
            return original(self, view[:1], flags)
        return original(self, view, flags)

    monkeypatch.setattr(socket.socket, "send", send)
    sink = SocketEventSink(collector.path)
    assert sink.emit({"n": 1, "pad": "abcdef"}) is True
    assert json.loads(collector.wait_for_lines(1)[0])["n"] == 1


def test_lock_timeout_while_a_write_blocks() -> None:
    with LineCollector(hold_reads=True) as collector:
        sink = SocketEventSink(collector.path)
        outcome: list[bool] = []

        def blocked() -> None:
            # Larger than any default Unix socket buffer (Linux buffers ~200 KiB), so
            # this write blocks and holds the sink lock until the 2 s write timeout.
            outcome.append(sink.emit({"pad": "x" * 8_000_000}))

        thread = threading.Thread(target=blocked)
        thread.start()
        time.sleep(0.2)
        started = time.monotonic()
        assert sink.emit({"n": 2}, lock_timeout=0.05) is False
        assert time.monotonic() - started < 0.4
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert outcome == [False]


def test_concurrent_emits_do_not_interleave_lines(collector: LineCollector) -> None:
    sink = SocketEventSink(collector.path)
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            barrier.wait(timeout=2)
            for number in range(10):
                if not sink.emit({"thread": index, "n": number, "pad": "y" * 32}):
                    raise RuntimeError(f"emit failed {index} {number}")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    assert errors == []
    lines = collector.wait_for_lines(80)
    assert len(lines) == 80
    seen = {(item["thread"], item["n"]) for item in (json.loads(line) for line in lines)}
    assert seen == {(index, number) for index in range(8) for number in range(10)}
    assert collector.accepts == 1


def test_logging_handler_reentry_does_not_recurse() -> None:
    directory = tempfile.TemporaryDirectory(prefix="lcq", dir="/tmp")
    path = os.path.join(directory.name, "missing.sock")
    sink = SocketEventSink(path)
    logger = logging.getLogger("laravel_cloud_queues.observability")
    messages: list[str] = []

    class Handler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def emit(self, record: logging.LogRecord) -> None:
            self.calls += 1
            messages.append(record.getMessage())
            sink.emit({"payload": "secret-payload"})

    handler = Handler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        assert sink.emit({"payload": "secret-payload"}) is False
    finally:
        logger.removeHandler(handler)
        directory.cleanup()
    assert handler.calls == 1
    assert messages
    assert all("secret-payload" not in message for message in messages)
