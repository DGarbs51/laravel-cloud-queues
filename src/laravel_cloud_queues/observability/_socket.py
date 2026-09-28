"""Persistent Unix-socket writer for Laravel Cloud log events.

Matches ``Illuminate\\Foundation\\Cloud\\Events``: 2 s connect timeout, 2 s write
timeout, EOF check and reconnect before each write, a partial-write loop, and
give-up after 5 consecutive zero-byte writes. Failures are logged locally and
never raised.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Mapping
from contextlib import suppress

from ._events import encode_event_line
from ._guard import begin_call, end_call, log_failure

_CONNECT_TIMEOUT_SECONDS = 2.0
_WRITE_TIMEOUT_SECONDS = 2.0
_ZERO_WRITE_LIMIT = 5
_PEEK_FLAGS = socket.MSG_PEEK
if hasattr(socket, "MSG_DONTWAIT"):
    _PEEK_FLAGS |= socket.MSG_DONTWAIT


def _unix_path(address: str) -> str:
    """Accept ``unix:///path`` or a bare filesystem path."""

    prefix = "unix://"
    if address.startswith(prefix):
        return address[len(prefix) :]
    return address


def _peer_closed(sock: socket.socket) -> bool:
    """Non-blocking peek. True when the peer has closed or the socket is unusable."""

    try:
        sock.setblocking(False)
        try:
            peeked = sock.recv(1, _PEEK_FLAGS)
        except BlockingIOError:
            return False
        except InterruptedError:
            return False
        except OSError:
            return True
        return peeked == b""
    except OSError:
        return True
    finally:
        with suppress(OSError):
            sock.settimeout(_WRITE_TIMEOUT_SECONDS)


class SocketEventSink:
    """Persistent Unix stream socket writer (Laravel ``Cloud\\Events``): 2 s connect and write
    timeouts, EOF check + reconnect before each write, partial-write loop, give up after 5
    zero-byte writes, one line per event, thread-safe (lines never interleave)."""

    def __init__(self, address: str) -> None:
        """``address``: ``unix:///path`` or a bare path."""

        self._path = _unix_path(address)
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Write one NDJSON line. Returns False on failure; never raises."""

        if not begin_call():
            return False
        acquired = False
        try:
            acquired = self._acquire(lock_timeout)
            if not acquired:
                log_failure("observability socket lock timed out")
                return False
            return self._emit_locked(event)
        except Exception:
            log_failure("observability socket emit failed")
            return False
        finally:
            if acquired:
                self._lock.release()
            end_call()

    def close(self) -> None:
        if not begin_call():
            return
        acquired = False
        try:
            acquired = self._lock.acquire()
            if acquired:
                self._disconnect()
        except Exception:
            log_failure("observability socket close failed")
        finally:
            if acquired:
                self._lock.release()
            end_call()

    def _acquire(self, lock_timeout: float | None) -> bool:
        if lock_timeout is None:
            return self._lock.acquire()
        return self._lock.acquire(timeout=lock_timeout)

    def _emit_locked(self, event: Mapping[str, object]) -> bool:
        try:
            line = encode_event_line(event)
        except Exception:
            log_failure("observability event encoding failed")
            return False
        sock = self._usable_socket()
        if sock is None:
            log_failure("observability socket connect failed")
            return False
        if not self._send_all(sock, line):
            log_failure("observability socket write failed")
            return False
        return True

    def _usable_socket(self) -> socket.socket | None:
        current = self._sock
        if current is not None:
            if not _peer_closed(current):
                return current
            self._disconnect()
        try:
            connected = self._connect()
        except OSError:
            self._sock = None
            return None
        self._sock = connected
        return connected

    def _connect(self) -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(_CONNECT_TIMEOUT_SECONDS)
            sock.connect(self._path)
            sock.settimeout(_WRITE_TIMEOUT_SECONDS)
        except OSError:
            with suppress(OSError):
                sock.close()
            raise
        return sock

    def _send_all(self, sock: socket.socket, payload: bytes) -> bool:
        view = memoryview(payload)
        offset = 0
        zeros = 0
        total = len(payload)
        while offset < total:
            try:
                sent = sock.send(view[offset:])
            except InterruptedError:
                continue
            except OSError:
                self._disconnect()
                return False
            if sent == 0:
                zeros += 1
                if zeros >= _ZERO_WRITE_LIMIT:
                    self._disconnect()
                    return False
                continue
            zeros = 0
            offset += sent
        return True

    def _disconnect(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is None:
            return
        try:
            sock.close()
        except OSError:
            return
