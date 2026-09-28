"""Stateful, repository-only implementation of the public Cloud agent protocol."""

from __future__ import annotations

import argparse
import copy
import json
import math
import os
import signal
import threading
import time
import uuid
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler
from typing import Literal, cast

from typing_extensions import Self

from harness._socket import SocketService, UnixServer

DEFAULT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/default"
Endpoint = Literal["next", "result"]


@dataclass(frozen=True)
class Fault:
    kind: str
    code: int = 200
    body: str = ""
    seconds: float = 0


def status(code: int, body: str = "") -> Fault:
    return Fault("status", code=code, body=body)


def delay(seconds: float) -> Fault:
    return Fault("delay", seconds=seconds)


@dataclass
class Message:
    message_id: str
    body: str
    queue_url: str
    visible_at: float
    receipt_handle: str | None = None
    receive_count: int = 0
    status: str = "pending"
    history: list[str] = field(default_factory=lambda: ["queued"])


@dataclass
class Result:
    body: object
    raw_body: bytes
    response_code: int | None = None
    applied_code: int | None = None
    completed: bool = False


class AgentEmulator(SocketService):
    def __init__(
        self,
        socket_path: str | os.PathLike[str] | None = None,
        *,
        poll_wait: float = 1.0,
        visibility_timeout: float | None = 30,
        unknown_message_status: int = 404,
        stale_receipt_status: int = 404,
    ) -> None:
        if not math.isfinite(poll_wait) or poll_wait < 0:
            raise ValueError("poll_wait must be finite and non-negative")
        if visibility_timeout is not None and (
            not math.isfinite(visibility_timeout) or visibility_timeout < 0
        ):
            raise ValueError("visibility_timeout must be finite and non-negative, or None")
        super().__init__(socket_path)
        self.poll_wait = poll_wait
        self.visibility_timeout = visibility_timeout
        self.unknown_message_status = unknown_message_status
        self.stale_receipt_status = stale_receipt_status
        self._condition = threading.Condition()
        self._stopped = threading.Event()
        self._messages: dict[str, Message] = {}
        self._results: list[Result] = []
        self._faults: dict[str, deque[Fault]] = {"next": deque(), "result": deque()}
        self._faults_fired: list[tuple[str, Fault]] = []

    @classmethod
    def start(
        cls,
        socket_path: str | os.PathLike[str] | None = None,
        *,
        poll_wait: float = 1.0,
        visibility_timeout: float | None = 30,
        unknown_message_status: int = 404,
        stale_receipt_status: int = 404,
    ) -> AgentEmulator:
        emulator = cls(
            socket_path,
            poll_wait=poll_wait,
            visibility_timeout=visibility_timeout,
            unknown_message_status=unknown_message_status,
            stale_receipt_status=stale_receipt_status,
        )
        return emulator.__enter__()

    def __enter__(self) -> Self:
        if self._server is None:
            self._stopped.clear()
            try:
                self._listen(_AgentServer(self.socket_path, self))
            except BaseException:
                self.close()
                raise
        return self

    def stop(self) -> None:
        self._stopped.set()
        with self._condition:
            self._condition.notify_all()
        super().stop()

    def enqueue(self, body: str, *, queue_url: str = DEFAULT_QUEUE_URL, delay: float = 0) -> str:
        if not isinstance(body, str):
            raise TypeError("body must be a string")
        if not isinstance(queue_url, str):
            raise TypeError("queue_url must be a string")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be finite and non-negative")
        message_id = str(uuid.uuid4())
        with self._condition:
            self._messages[message_id] = Message(
                message_id, body, queue_url, time.monotonic() + delay
            )
            self._condition.notify_all()
        return message_id

    @property
    def results(self) -> list[Result]:
        with self._condition:
            return copy.deepcopy(self._results)

    @property
    def faults_fired(self) -> list[tuple[str, Fault]]:
        with self._condition:
            return list(self._faults_fired)

    def wait_for_result(self, message_id: str, timeout: float = 5) -> Result:
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                for result in self._results:
                    if (
                        result.completed
                        and isinstance(result.body, dict)
                        and result.body.get("messageId") == message_id
                    ):
                        return copy.deepcopy(result)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"No result for {message_id}")
                self._condition.wait(remaining)

    def _expire(self) -> None:
        for message in self._messages.values():
            if message.status == "in_flight" and message.visible_at <= time.monotonic():
                message.status = "pending"
                message.history.append("expired")

    def message(self, message_id: str) -> Message:
        with self._condition:
            self._expire()
            return copy.deepcopy(self._messages[message_id])

    def in_flight(self) -> list[Message]:
        return self._with_status("in_flight")

    def pending(self) -> list[Message]:
        """Undeleted, unreserved messages, including messages still delayed."""
        return self._with_status("pending")

    def _with_status(self, status: str) -> list[Message]:
        with self._condition:
            self._expire()
            return copy.deepcopy([m for m in self._messages.values() if m.status == status])

    def inject(self, endpoint: Endpoint, fault: Fault | str, times: int = 1) -> None:
        if endpoint not in self._faults or type(times) is not int or times < 1:
            raise ValueError("Expected next/result and a positive integer times")
        fault = Fault(fault) if isinstance(fault, str) else fault
        if fault.kind not in {
            "disconnect",
            "status",
            "malformed_json",
            "non_object_json",
            "missing_message_id",
            "empty_message_id",
            "non_string_fields",
            "delay",
            "apply_then_disconnect",
            "hang",
        }:
            raise ValueError(f"Unknown fault: {fault.kind}")
        if fault.kind == "apply_then_disconnect" and endpoint != "result":
            raise ValueError("apply_then_disconnect requires result")
        if not math.isfinite(fault.seconds) or fault.seconds < 0:
            raise ValueError("Fault delay must be finite and non-negative")
        with self._condition:
            self._faults[endpoint].extend([fault] * times)

    def _take_fault(self, endpoint: str) -> Fault | None:
        with self._condition:
            fault = self._faults[endpoint].popleft() if self._faults[endpoint] else None
            if fault is not None:
                self._faults_fired.append((endpoint, fault))
            return fault

    def _next(self) -> tuple[int, object]:
        deadline = time.monotonic() + self.poll_wait
        with self._condition:
            while not self._stopped.is_set():
                self._expire()
                now = time.monotonic()
                for message in self._messages.values():
                    if message.status == "pending" and message.visible_at <= now:
                        message.receive_count += 1
                        message.receipt_handle = str(uuid.uuid4())
                        message.visible_at = (
                            math.inf
                            if self.visibility_timeout is None
                            else now + self.visibility_timeout
                        )
                        message.status = "in_flight"
                        message.history.append("delivered")
                        return 200, {
                            "messageId": message.message_id,
                            "receiptHandle": message.receipt_handle,
                            "body": message.body,
                            "attributes": {"ApproximateReceiveCount": str(message.receive_count)},
                            "queueUrl": message.queue_url,
                        }
                remaining = deadline - now
                if remaining <= 0:
                    break
                next_visible = min(
                    (m.visible_at for m in self._messages.values() if m.status != "processed"),
                    default=math.inf,
                )
                self._condition.wait(min(remaining, max(0, next_visible - now)))
        return 204, None

    def _apply(self, body: object) -> int:
        if not isinstance(body, dict):
            return 422
        if (
            set(body) - {"messageId", "receiptHandle", "status", "delay"}
            or not isinstance(body.get("messageId"), str)
            or body.get("status") not in ("processed", "released")
            or ("receiptHandle" in body and not isinstance(body["receiptHandle"], str))
            or (
                "delay" in body
                and (
                    body["status"] != "released"
                    or type(body["delay"]) is not int
                    or not 0 <= body["delay"] <= 43200
                )
            )
        ):
            return 422
        with self._condition:
            self._expire()
            message = self._messages.get(body["messageId"])
            if message is None or message.status == "processed":
                return self.unknown_message_status
            if message.status != "in_flight" or (
                body.get("receiptHandle") != message.receipt_handle
            ):
                return self.stale_receipt_status
            message.history.append(body["status"])
            message.status = "processed" if body["status"] == "processed" else "pending"
            message.visible_at = time.monotonic() + body.get("delay", 0)
            self._condition.notify_all()
            return 200


class _AgentServer(UnixServer):
    def __init__(self, path: str, emulator: AgentEmulator) -> None:
        self.emulator = emulator
        super().__init__(path, _AgentHandler)


class _AgentHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        pass

    def handle(self) -> None:
        # Client disconnects are intentional in this harness.
        with suppress(OSError):
            super().handle()

    def do_GET(self) -> None:
        self._respond("next" if self.path == "/next" else None)

    def do_POST(self) -> None:
        self._respond("result" if self.path == "/result" else None)

    def _respond(self, endpoint: str | None) -> None:
        if endpoint is None:
            self.close_connection = True
            self._send(404, b"")
            return
        emulator = cast(_AgentServer, self.server).emulator
        record: Result | None = None
        if endpoint == "result":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or self.headers.get("Transfer-Encoding"):
                    raise ValueError("Invalid request framing")
            except ValueError:
                self.close_connection = True
                length = 0
            raw = self.rfile.read(length)
            try:
                body: object = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                body = None
            record = Result(body, raw)
            with emulator._condition:
                emulator._results.append(record)
                emulator._condition.notify_all()
        fault = emulator._take_fault(endpoint)
        if fault is not None:
            if fault.kind == "delay":
                emulator._stopped.wait(fault.seconds)
            elif fault.kind == "hang":
                emulator._stopped.wait()
            elif fault.kind == "apply_then_disconnect":
                assert record is not None
                with emulator._condition:
                    record.applied_code = emulator._apply(record.body)
            if fault.kind in {"disconnect", "apply_then_disconnect", "hang"}:
                self._record_response(emulator, record, None)
                self.close_connection = True
                return
            if fault.kind != "delay":
                code, raw = self._fault_response(fault)
                self._record_response(emulator, record, code)
                self._send(code, raw)
                return
        if emulator._stopped.is_set():
            self._record_response(emulator, record, None)
            self.close_connection = True
            return
        if record is not None:
            with emulator._condition:
                code = emulator._apply(record.body)
                record.applied_code = code
            response: object = {}
        else:
            code, response = emulator._next()
        self._record_response(emulator, record, code)
        self._send(code, b"" if code == 204 else json.dumps(response).encode())

    def _record_response(
        self, emulator: AgentEmulator, record: Result | None, code: int | None
    ) -> None:
        if record is not None:
            with emulator._condition:
                record.response_code = code
                record.completed = True
                emulator._condition.notify_all()

    def _fault_response(self, fault: Fault) -> tuple[int, bytes]:
        if fault.kind == "status":
            return fault.code, fault.body.encode()
        responses = {
            "malformed_json": b"{invalid",
            "non_object_json": b'"x"',
            "missing_message_id": b"{}",
            "empty_message_id": b'{"messageId":""}',
            "non_string_fields": b'{"messageId":"fault-message","receiptHandle":42,"body":[]}',
        }
        return 200, responses[fault.kind]

    def _send(self, code: int, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    args = parser.parse_args()
    stopped = threading.Event()

    def stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with AgentEmulator.start(args.socket):
        print(f"Agent emulator listening on {args.socket}", flush=True)
        stopped.wait()


if __name__ == "__main__":
    main()
