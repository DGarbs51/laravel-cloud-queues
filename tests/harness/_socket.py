"""Shared ownership and shutdown rules for the two local Unix servers."""

from __future__ import annotations

import contextlib
import os
import socket
import socketserver
import tempfile
import threading
from pathlib import Path
from types import TracebackType


class UnixServer(socketserver.ThreadingUnixStreamServer):
    request_queue_size = socket.SOMAXCONN

    def __init__(self, path: str, handler: type[socketserver.BaseRequestHandler]) -> None:
        self.clients: set[socket.socket] = set()
        self.client_lock = threading.Lock()
        super().__init__(path, handler, bind_and_activate=False)
        self.path = Path(path)
        self.identity: tuple[int, int] | None = None
        try:
            self.server_bind()  # bind fails on existing paths; never unlink to make room.
            stat = self.path.lstat()
            self.identity = (stat.st_dev, stat.st_ino)
            self.server_activate()
        except BaseException:
            self.server_close()
            raise

    def get_request(self) -> tuple[socket.socket, str]:
        client, address = self.socket.accept()
        with self.client_lock:
            self.clients.add(client)
        return client, str(address)

    def process_request_thread(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: str
    ) -> None:
        assert isinstance(request, socket.socket)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self.client_lock:
                self.clients.discard(request)

    def close_clients(self) -> None:
        with self.client_lock:
            for client in self.clients:
                with contextlib.suppress(OSError):
                    client.shutdown(socket.SHUT_RDWR)
                client.close()

    def server_close(self) -> None:
        self.close_clients()
        super().server_close()
        if self.identity is not None:
            try:
                stat = self.path.lstat()
                if (stat.st_dev, stat.st_ino) == self.identity:
                    self.path.unlink()
            except FileNotFoundError:
                pass
            self.identity = None


class SocketService:
    def __init__(self, socket_path: str | os.PathLike[str] | None) -> None:
        self._directory = Path(tempfile.mkdtemp(prefix="lcq-")) if socket_path is None else None
        self.socket_path = str(self._directory / "socket" if self._directory else socket_path)
        self._server: UnixServer | None = None
        self._thread: threading.Thread | None = None

    def _listen(self, server: UnixServer) -> None:
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, args=(0.02,), daemon=True)
        self._thread.start()

    def close_clients(self) -> None:
        if self._server is not None:
            self._server.close_clients()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def close(self) -> None:
        self.stop()
        if self._directory is not None:
            self._directory.rmdir()
            self._directory = None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
