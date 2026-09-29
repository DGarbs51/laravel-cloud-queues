"""The persistent Unix socket writer for Laravel Cloud log events.

It matches ``Illuminate\\Foundation\\Cloud\\Events``: a 2 second connect timeout, a
2 second write timeout, an EOF check and reconnect before each write, a partial write
loop, and giving up after 5 consecutive zero-byte writes. Failures are logged locally
and never raised.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Mapping
from contextlib import suppress

from ._events import encode_event_line
from ._guard import begin_call, end_call, log_failure

_CONNECT_TIMEOUT_SECONDS = 2.0
"""The number of seconds to wait while connecting to the socket."""
_WRITE_TIMEOUT_SECONDS = 2.0
"""The number of seconds to wait for each write to the socket."""
_ZERO_WRITE_LIMIT = 5
"""The number of consecutive zero-byte writes before giving up."""
_PEEK_FLAGS = socket.MSG_PEEK | getattr(socket, "MSG_DONTWAIT", 0)
"""The flags used to peek at the socket without blocking."""


def _unix_path(address: str) -> str:
    """Get the filesystem path from a ``unix:///path`` address or a bare path."""

    prefix = "unix://"
    if address.startswith(prefix):
        return address[len(prefix) :]
    return address


def _peer_closed(sock: socket.socket) -> bool:
    """Determine if the peer has closed the socket or the socket is unusable.

    This performs a non-blocking peek and restores the write timeout afterwards.
    """

    try:
        sock.setblocking(False)
        try:
            peeked = sock.recv(1, _PEEK_FLAGS)
        except BlockingIOError:
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
    """A persistent Unix stream socket writer for Cloud events.

    It mirrors Laravel's ``Cloud\\Events`` writer and writes one line per event. It is
    thread-safe, so lines from concurrent emits never interleave.
    """

    def __init__(self, address: str) -> None:
        """Create a new socket event sink instance.

        The address may be a ``unix:///path`` URL or a bare filesystem path.
        """

        self._path = _unix_path(address)
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    def emit(self, event: Mapping[str, object], *, lock_timeout: float | None = None) -> bool:
        """Write the event to the socket as a single NDJSON line.

        Returns False on failure and never raises. ``lock_timeout`` bounds the wait for
        the write lock.
        """

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
        """Close the socket connection, if one is open."""
        if not begin_call():
            return
        try:
            with self._lock:
                self._disconnect()
        except Exception:
            log_failure("observability socket close failed")
        finally:
            end_call()

    def _acquire(self, lock_timeout: float | None) -> bool:
        """Acquire the write lock, waiting at most the given number of seconds."""
        if lock_timeout is None:
            return self._lock.acquire()
        return self._lock.acquire(timeout=lock_timeout)

    def _emit_locked(self, event: Mapping[str, object]) -> bool:
        """Encode and write the event while holding the write lock."""
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
        """Get a connected socket, reconnecting if the peer has closed it."""
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
        """Open a new connection to the socket path."""
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
        """Write the entire payload to the socket.

        The connection is dropped on error or after too many consecutive zero-byte writes.
        """
        view = memoryview(payload)
        offset = 0
        zeros = 0
        total = len(payload)
        while offset < total:
            try:
                sent = sock.send(view[offset:])
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
        """Close and forget the current socket."""
        sock = self._sock
        self._sock = None
        if sock is None:
            return
        try:
            sock.close()
        except OSError:
            return
