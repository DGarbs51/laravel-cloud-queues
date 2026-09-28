"""Laravel Cloud in-container agent consumer (managed mode)."""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Sequence
from contextlib import suppress
from time import monotonic, sleep
from typing import Any
from weakref import WeakValueDictionary

import httpx

from ...config import ManagedQueuesConfig
from ...errors import AgentProtocolError, AgentUnavailableError
from ..base import Delivery
from ..sqs import normalize_queue

_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_CONNECTION_ERRORS = (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError)


class AgentConsumer:
    """Synchronous HTTP over the configured Unix socket, with no redirects or proxies.

    Polls retry connection failures and HTTP errors three times (0, 500 ms).
    Results retry only connection failures three times (100 ms apart). An outcome
    is claimed before sending, including when its acknowledgement is ambiguous.
    ``interrupt`` permanently stops polling but leaves result reporting available.
    """

    def __init__(self, managed: ManagedQueuesConfig) -> None:
        self._managed = managed
        self._client = httpx.Client(
            transport=httpx.HTTPTransport(uds=managed.agent.socket),
            base_url="http://localhost",
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._receiving = False
        self._socket: socket.socket | None = None
        # Identity-based weak values avoid retaining bodies for the worker's lifetime.
        self._reported: WeakValueDictionary[int, Delivery] = WeakValueDictionary()

    @property
    def supports_renewal(self) -> bool:
        return False

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        with self._lock:
            if self._stopping.is_set():
                return None
            self._receiving = True
        try:
            for attempt in range(3):
                if self._stopping.is_set():
                    return None
                try:
                    status, body = self._request("GET", "/next", timeout=65)
                except _CONNECTION_ERRORS:
                    if self._stopping.is_set():
                        return None
                    if attempt == 2:
                        raise AgentUnavailableError(
                            "The agent runtime socket is unreachable."
                        ) from None
                else:
                    if status == 204:
                        return None
                    if status == 200:
                        # Do not check stopping here: a handed-over message must run.
                        return self._delivery(body)
                    if not 400 <= status < 600 or attempt == 2:
                        raise AgentUnavailableError(
                            f"The agent returned HTTP {status} from GET /next."
                        )
                if attempt == 1 and self._stopping.wait(0.5):
                    return None
            return None  # All attempts return, raise, or stop above.
        finally:
            with self._lock:
                self._receiving = False

    def _delivery(self, body: bytes) -> Delivery | None:
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError, RecursionError):
            raise AgentUnavailableError("The agent returned invalid JSON from GET /next.") from None
        if not isinstance(data, (dict, list)):
            raise AgentUnavailableError("The agent returned non-object/array JSON from GET /next.")
        if not isinstance(data, dict):
            return None
        message_id = data.get("messageId")
        if not isinstance(message_id, str) or not message_id:
            return None
        receipt = data.get("receiptHandle")
        payload = data.get("body")
        attributes = data.get("attributes")
        count = attributes.get("ApproximateReceiveCount") if isinstance(attributes, dict) else None
        # Labeled deviation: missing-receive-count-is-one (D13.5, Symfony parity).
        try:
            attempt = max(1, int(count)) if isinstance(count, (str, int, float)) else 1
        except (ValueError, OverflowError):
            attempt = 1
        queue_url = data.get("queueUrl")
        queue = self._managed.queue
        meta = {}
        if isinstance(queue_url, str) and queue_url:
            queue = normalize_queue(self._managed.connection, queue_url)
            meta["queue_url"] = queue_url
        return Delivery(
            message_id=message_id,
            receipt=receipt if isinstance(receipt, str) else None,
            body=payload if isinstance(payload, str) else "",
            attempt=attempt,
            queue=queue,
            received_at=monotonic(),
            meta=meta,
        )

    def complete(self, delivery: Delivery) -> None:
        self._report(delivery, "processed")

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        self._report(delivery, "released", delay_seconds)

    def _report(self, delivery: Delivery, status: str, delay: int | None = None) -> None:
        with self._lock:
            if id(delivery) in self._reported:
                raise RuntimeError("This delivery already has a reported outcome.")
            self._reported[id(delivery)] = delivery
        payload: dict[str, str | int] = {"messageId": delivery.message_id, "status": status}
        if delivery.receipt is not None:
            payload["receiptHandle"] = delivery.receipt
        if delay is not None:
            payload["delay"] = delay
        for attempt in range(3):
            try:
                code, _ = self._request("POST", "/result", timeout=10, payload=payload)
            except _CONNECTION_ERRORS:
                if attempt == 2:
                    raise AgentUnavailableError(
                        "The agent runtime socket is unreachable."
                    ) from None
                sleep(0.1)
                continue
            if code >= 500:
                raise AgentUnavailableError(f"The agent returned HTTP {code} from POST /result.")
            if code >= 400:
                raise AgentProtocolError(
                    f"The agent rejected the result with HTTP {code}.", status=code
                )
            return

    def _request(
        self, method: str, path: str, *, timeout: float, payload: dict[str, str | int] | None = None
    ) -> tuple[int, bytes]:
        try:
            with self._client.stream(
                method, path, json=payload, timeout=timeout, extensions={"trace": self._trace}
            ) as response:
                # Only successful polls have a body we use. Never buffer error/result bodies.
                if method != "GET" or response.status_code != 200:
                    return response.status_code, b""
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise AgentUnavailableError(
                        "The agent returned an unsupported content encoding."
                    )
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
                        raise AgentUnavailableError("The agent response exceeds the size limit.")
                    body.extend(chunk)
                return response.status_code, bytes(body)
        except _CONNECTION_ERRORS:
            raise
        except httpx.HTTPError:
            raise AgentUnavailableError("The agent HTTP request failed.") from None

    def _trace(self, event: str, info: dict[str, Any]) -> None:
        # httpx's trace extension supplies an opaque httpcore network stream (Any).
        # Capture its socket before response headers: pool.close() alone does not
        # reliably wake a blocking recv on all supported platforms; shutdown does.
        if event == "connection.connect_unix_socket.complete":
            sock = info["return_value"].get_extra_info("socket")
            if isinstance(sock, socket.socket):
                with self._lock:
                    self._socket = sock
                    if self._receiving and self._stopping.is_set():
                        self._shutdown_socket()

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """No-op: the agent owns visibility heartbeats for its deliveries."""

    def _shutdown_socket(self) -> None:
        if self._socket is not None:
            with suppress(OSError):
                self._socket.shutdown(socket.SHUT_RDWR)

    def interrupt(self) -> None:
        with self._lock:
            self._stopping.set()
            if self._receiving:
                self._shutdown_socket()

    def close(self) -> None:
        self.interrupt()
        self._client.close()
