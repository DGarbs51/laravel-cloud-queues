"""Persistent NDJSON capture and pure validators for the Cloud event contract."""

from __future__ import annotations

import copy
import json
import os
import re
import socketserver
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import cast

from typing_extensions import Self

from harness._socket import SocketService, UnixServer

_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}")


def _timestamp(value: object) -> bool:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return False
    return True


def validate_lifecycle_event(obj: object) -> list[str]:
    if not isinstance(obj, dict):
        return ["event must be an object"]
    errors: list[str] = []
    if obj.keys() - {"_cloud_event", "type", "queue", "timestamp", "duration_ms"}:
        errors.append("unknown lifecycle keys")
    if obj.get("_cloud_event") != "queue":
        errors.append("_cloud_event must be queue")
    kind = obj.get("type")
    if kind not in ("queued", "started", "processed", "released", "failed"):
        errors.append("invalid lifecycle type")
    if not _timestamp(obj.get("timestamp")):
        errors.append("timestamp must have six microsecond digits and no timezone suffix")
    if not isinstance(obj.get("queue"), str):
        errors.append("queue must be a string")
    if kind in ("processed", "released", "failed"):
        duration = obj.get("duration_ms")
        if type(duration) is not int or duration < 0:
            errors.append("duration_ms must be a non-negative integer on completion")
    elif "duration_ms" in obj:
        errors.append("duration_ms is only allowed on completion")
    return errors


def validate_failed_job_event(obj: object) -> list[str]:
    if not isinstance(obj, dict):
        return ["event must be an object"]
    errors: list[str] = []
    if obj.keys() - {
        "_cloud_event",
        "id",
        "queue",
        "started_at",
        "attempts",
        "payload",
        "exception_preview",
        "job_name",
        "exception",
        "replayable",
    }:
        errors.append("unknown failed_job keys")
    if obj.get("_cloud_event") != "failed_job":
        errors.append("_cloud_event must be failed_job")
    for name in ("id", "queue", "payload", "exception_preview", "job_name", "exception"):
        if not isinstance(obj.get(name), str):
            errors.append(f"{name} must be a string")
    identifier = obj.get("id")
    try:
        parsed = uuid.UUID(identifier) if isinstance(identifier, str) else None
        if parsed is None or parsed.version != 7:
            errors.append("id must be a UUIDv7")
    except ValueError:
        errors.append("id must be a UUIDv7")
    if not _timestamp(obj.get("started_at")):
        errors.append("started_at must have six microsecond digits and no timezone suffix")
    attempts = obj.get("attempts")
    if type(attempts) is not int or attempts < 1:
        errors.append("attempts must be a positive integer")
    preview = obj.get("exception_preview")
    if isinstance(preview, str) and len(preview) > 1001:
        errors.append("exception_preview exceeds 1001 characters")
    if "replayable" in obj and obj["replayable"] is not False:
        errors.append("replayable, when present, must be false")
    return errors


def validate_sequence(events: Sequence[object], expected_types: Sequence[str]) -> list[str]:
    """Validate every event, then compare types (failed-job records use 'failed_job')."""
    errors: list[str] = []
    actual: list[object] = []
    for index, event in enumerate(events):
        failed = isinstance(event, dict) and event.get("_cloud_event") == "failed_job"
        validator = validate_failed_job_event if failed else validate_lifecycle_event
        errors.extend(f"event {index}: {error}" for error in validator(event))
        actual.append(
            "failed_job" if failed else event.get("type") if isinstance(event, dict) else None
        )
    if actual != list(expected_types):
        errors.append(f"expected sequence {list(expected_types)!r}, got {actual!r}")
    return errors


def _reject_constant(value: str) -> None:
    raise ValueError(f"Non-JSON constant: {value}")


class LogCollector(SocketService):
    def __init__(
        self, socket_path: str | os.PathLike[str] | None = None, *, refuse: bool = False
    ) -> None:
        super().__init__(socket_path)
        self._refuse = refuse
        self._condition = threading.Condition()
        self._raw_lines: list[bytes] = []
        self._events: list[object] = []
        self._errors: list[str] = []

    @property
    def refuse(self) -> bool:
        return self._refuse

    @refuse.setter
    def refuse(self, value: bool) -> None:
        self._refuse = value
        if value:
            self.stop()

    def start(self) -> Self:
        if self._server is None and not self.refuse:
            self._listen(_CollectorServer(self.socket_path, self))
        return self

    def __enter__(self) -> Self:
        try:
            return self.start()
        except BaseException:
            self.close()
            raise

    @property
    def raw_lines(self) -> list[bytes]:
        """Complete lines include their newline; trailing partials retain their exact bytes."""
        with self._condition:
            return list(self._raw_lines)

    @property
    def events(self) -> list[object]:
        with self._condition:
            return copy.deepcopy(self._events)

    @property
    def errors(self) -> list[str]:
        with self._condition:
            return list(self._errors)

    def wait_for(
        self, predicate: Callable[[list[object]], bool], timeout: float = 5
    ) -> list[object]:
        deadline = time.monotonic() + timeout
        with self._condition:
            while not predicate(self.events):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Collector predicate was not satisfied")
                self._condition.wait(remaining)
            return self.events

    def _capture(self, line: bytes, *, partial: bool = False) -> None:
        with self._condition:
            index = len(self._raw_lines)
            self._raw_lines.append(line)
            if len(line) > 16384:
                self._errors.append(f"line {index}: exceeds 16384-byte limit including newline")
            if partial:
                self._errors.append(f"line {index}: partial line at EOF")
            else:
                try:
                    obj = json.loads(line.decode("utf-8"), parse_constant=_reject_constant)
                except (ValueError, UnicodeDecodeError):
                    self._errors.append(f"line {index}: invalid JSON or UTF-8")
                else:
                    self._events.append(obj)
                    if not isinstance(obj, dict):
                        self._errors.append(f"line {index}: event is not an object")
            self._condition.notify_all()


class _CollectorServer(UnixServer):
    def __init__(self, path: str, collector: LogCollector) -> None:
        self.collector = collector
        super().__init__(path, _CollectorHandler)


class _CollectorHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        collector = cast(_CollectorServer, self.server).collector
        pending = bytearray()
        try:
            while chunk := self.request.recv(65536):
                pending.extend(chunk)
                while (end := pending.find(b"\n")) != -1:
                    collector._capture(bytes(pending[: end + 1]))
                    del pending[: end + 1]
        except OSError:
            pass
        finally:
            if pending:
                collector._capture(bytes(pending), partial=True)
