"""The consumer that receives jobs from the Laravel Cloud in-container agent in managed mode."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Sequence
from contextlib import suppress
from time import monotonic, sleep
from weakref import WeakValueDictionary

import anyio
import httpx

from ...config import ManagedQueuesConfig
from ...errors import AgentProtocolError, AgentUnavailableError
from ..base import Delivery, json_object
from ..sqs import normalize_queue, receive_count

_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
"""The maximum size in bytes of a ``GET /next`` response body.

A 1 MiB SQS body can double in size under the agent's JSON escaping, so a 2 MiB limit
would reject legal messages, and a rejected message loops forever as poison.
"""
_CONNECTION_ERRORS = (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError)
"""The HTTP errors treated as connection failures and eligible for retry."""
_UNREACHABLE = "The agent runtime socket is unreachable."
"""The error message used once the connection retries are exhausted."""


class _OutcomeClaims:
    """One reported outcome per delivery, shared by a consumer and its sync twin.

    Keys are delivery identities and values are weak, so a reported body does not
    live for the worker's lifetime. Either side's report blocks the other.
    """

    def __init__(self) -> None:
        """Create a new outcome registry."""
        self._lock = threading.Lock()
        self.reported: WeakValueDictionary[int, Delivery] = WeakValueDictionary()

    def claim(self, delivery: Delivery) -> None:
        """Reserve the delivery's single outcome.

        Raises ``RuntimeError`` when an outcome was already reserved, including by the
        sync twin.
        """
        with self._lock:
            if id(delivery) in self.reported:
                raise RuntimeError("This delivery already has a reported outcome.")
            self.reported[id(delivery)] = delivery


def _parse_delivery(managed: ManagedQueuesConfig, body: bytes) -> Delivery | None:
    """Parse a ``GET /next`` response body into a delivery.

    Returns ``None`` when the response carries no message, and raises an
    ``AgentUnavailableError`` when the body is not a JSON object or array.
    """
    try:
        data: object = json.loads(body)
    except (ValueError, UnicodeError, RecursionError):
        raise AgentUnavailableError("The agent returned invalid JSON from GET /next.") from None
    if isinstance(data, list):
        return None
    if not json_object(data):
        raise AgentUnavailableError("The agent returned non-object/array JSON from GET /next.")
    message_id = data.get("messageId")
    if not isinstance(message_id, str) or not message_id:
        return None
    receipt = data.get("receiptHandle")
    payload = data.get("body")
    attributes = data.get("attributes")
    count = attributes.get("ApproximateReceiveCount") if json_object(attributes) else None
    attempt = receive_count(count)  # missing-receive-count-is-one (D13.5)
    queue_url = data.get("queueUrl")
    queue = managed.queue
    meta: dict[str, str] = {}
    if isinstance(queue_url, str) and queue_url:
        queue = normalize_queue(managed.connection, queue_url)
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


def _result_payload(delivery: Delivery, status: str, delay: int | None) -> dict[str, str | int]:
    """Build the ``POST /result`` body, omitting only a missing receipt or delay."""
    payload: dict[str, str | int] = {"messageId": delivery.message_id, "status": status}
    if delivery.receipt is not None:
        payload["receiptHandle"] = delivery.receipt
    if delay is not None:
        payload["delay"] = delay
    return payload


def _retryable_poll_failure(status: int) -> str:
    """Return the retry message for a failed poll.

    Raises ``AgentUnavailableError`` when the status is outside 400-599. Callers handle
    200 and 204 themselves.
    """
    failure = f"The agent returned HTTP {status} from GET /next."
    if not 400 <= status < 600:
        raise AgentUnavailableError(failure)
    return failure


def _result_status(code: int) -> None:
    """Raise when a ``POST /result`` status is a failure. Accept every other status."""
    if code >= 500:
        raise AgentUnavailableError(f"The agent returned HTTP {code} from POST /result.")
    if code >= 400:
        raise AgentProtocolError(f"The agent rejected the result with HTTP {code}.", status=code)


def _reject_encoding(encoding: str | None) -> None:
    """Reject a content encoding other than identity."""
    if encoding != "identity":
        raise AgentUnavailableError("The agent returned an unsupported content encoding.")


def _take_chunk(body: bytearray, chunk: bytes) -> None:
    """Append a streamed chunk, or raise once the response exceeds the size limit."""
    if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
        raise AgentUnavailableError("The agent response exceeds the size limit.")
    body.extend(chunk)


class AgentConsumer:
    """A consumer that talks to the agent over its Unix socket.

    Requests are synchronous HTTP with no redirects or proxies. Polls make up to three
    attempts on connection failures and HTTP errors, waiting 500 ms before the third,
    while results make up to three attempts on connection failures only, 100 ms apart.
    An outcome is claimed before it is sent, even when its acknowledgement turns out to
    be ambiguous. :meth:`interrupt` permanently stops polling but leaves result
    reporting available. Like Laravel, it never aborts an in-flight ``GET /next``, since
    a message the agent is handing over must reach the worker, so an idle shutdown may
    wait up to the poll's 65 second timeout.
    """

    def __init__(
        self, managed: ManagedQueuesConfig, *, claims: _OutcomeClaims | None = None
    ) -> None:
        """Create a new agent consumer instance."""
        self._managed = managed
        self._client = httpx.Client(
            transport=httpx.HTTPTransport(uds=managed.agent.socket),
            base_url="http://localhost",
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )
        self._stopping = threading.Event()
        self._claims = claims if claims is not None else _OutcomeClaims()
        self._reported = self._claims.reported

    @property
    def supports_renewal(self) -> bool:
        """Determine if the watchdog should renew the lease on deliveries."""
        return False

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Poll the agent for the next assigned message.

        The given queues and wait time are ignored, since the agent's assignment is
        authoritative. Raises an ``AgentUnavailableError`` once the retries are exhausted.
        """
        failure = _UNREACHABLE
        for attempt in range(3):
            if self._stopping.is_set():
                return None
            try:
                status, body = self._request("GET", "/next", timeout=65)
            except _CONNECTION_ERRORS:
                if self._stopping.is_set():
                    return None
                failure = _UNREACHABLE
            else:
                if status == 204:
                    return None
                if status == 200:
                    # Do not check stopping here: a handed-over message must run.
                    return self._delivery(body)
                failure = _retryable_poll_failure(status)
            if attempt == 1 and self._stopping.wait(0.5):
                return None
        raise AgentUnavailableError(failure)

    def _delivery(self, body: bytes) -> Delivery | None:
        """Parse a ``GET /next`` response body into a delivery."""
        return _parse_delivery(self._managed, body)

    def complete(self, delivery: Delivery) -> None:
        """Report the delivery to the agent as processed."""
        self._report(delivery, "processed")

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Report the delivery to the agent as released after the given delay."""
        self._report(delivery, "released", delay_seconds)

    def _report(self, delivery: Delivery, status: str, delay: int | None = None) -> None:
        """Send the delivery's outcome to the agent.

        Each delivery may report only one outcome. Raises an ``AgentProtocolError`` when the
        agent rejects the result with a 4xx, and an ``AgentUnavailableError`` when it is
        unreachable or fails with a 5xx.
        """
        self._claims.claim(delivery)
        payload = _result_payload(delivery, status, delay)
        for attempt in range(3):
            try:
                code, _ = self._request("POST", "/result", timeout=10, payload=payload)
            except _CONNECTION_ERRORS:
                if attempt < 2:
                    sleep(0.1)
                continue
            _result_status(code)
            return
        raise AgentUnavailableError(_UNREACHABLE)

    def _request(
        self, method: str, path: str, *, timeout: float, payload: dict[str, str | int] | None = None
    ) -> tuple[int, bytes]:
        """Send a request to the agent and return its status code and body.

        Only a successful poll's body is read, and it is bounded by the response size limit.
        Connection failures are raised as-is so callers can retry them.
        """
        try:
            with self._client.stream(method, path, json=payload, timeout=timeout) as response:
                # Only successful polls have a body we use. Never buffer error/result bodies.
                if method != "GET" or response.status_code != 200:
                    return response.status_code, b""
                _reject_encoding(response.headers.get("content-encoding", "identity"))
                body = bytearray()
                for chunk in response.iter_bytes():
                    _take_chunk(body, chunk)
                return response.status_code, bytes(body)
        except _CONNECTION_ERRORS:
            raise
        except httpx.HTTPError:
            raise AgentUnavailableError("The agent HTTP request failed.") from None

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Do nothing, since the agent owns the visibility heartbeats for its deliveries."""

    def interrupt(self) -> None:
        """Stop polling once the current ``GET /next`` finishes.

        The in-flight poll is never aborted, but a retry backoff is woken immediately.
        Result reporting stays available. This is thread-safe.
        """
        self._stopping.set()

    def close(self) -> None:
        """Stop polling and close the HTTP client."""
        self.interrupt()
        self._client.close()


class AsyncAgentConsumer:
    """An async consumer that talks to the agent over its Unix socket.

    Poll and result semantics match :class:`AgentConsumer`: three poll attempts, 500 ms
    before the third, a 65 second poll timeout, three result attempts 100 ms apart on
    connection errors only, and a 4 MiB streamed body limit. An outcome is claimed in the
    registry shared with :attr:`blocking` before it is sent. :meth:`interrupt` is
    thread-safe, does not block, and never aborts an in-flight ``GET /next``; it only
    wakes the poll backoff. Result reporting stays available after it.
    """

    def __init__(self, managed: ManagedQueuesConfig) -> None:
        """Create a new async agent consumer and its sync twin."""
        self._managed = managed
        self._claims = _OutcomeClaims()
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=managed.agent.socket),
            base_url="http://localhost",
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )
        self._stopping = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._backoff: asyncio.Event | None = None
        # The twin must not open a socket until something calls it. httpx connects on use.
        self._blocking = AgentConsumer(managed, claims=self._claims)

    @property
    def supports_renewal(self) -> bool:
        """Determine if the worker should renew the lease on deliveries."""
        return False

    @property
    def blocking(self) -> AgentConsumer:
        """Get the synchronous twin that shares this consumer's outcome registry."""
        return self._blocking

    async def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Poll the agent for the next assigned message.

        The given queues and wait time are ignored, since the agent's assignment is
        authoritative. Raises an ``AgentUnavailableError`` once the retries are exhausted.
        An in-flight poll is never cancelled.
        """
        self._loop = asyncio.get_running_loop()
        failure = _UNREACHABLE
        for attempt in range(3):
            if self._stopping.is_set():
                return None
            try:
                status, body = await self._request("GET", "/next", http_timeout=65)
            except _CONNECTION_ERRORS:
                if self._stopping.is_set():
                    return None
                failure = _UNREACHABLE
            else:
                if status == 204:
                    return None
                if status == 200:
                    # Do not check stopping here: a handed-over message must run.
                    return _parse_delivery(self._managed, body)
                failure = _retryable_poll_failure(status)
            if attempt != 1:
                continue
            # Published before the wait so interrupt from another thread can set it.
            # A flag set during the poll sets the event too, or the wait would run out.
            event = asyncio.Event()
            self._backoff = event
            try:
                if self._stopping.is_set():
                    event.set()
                with anyio.move_on_after(0.5):
                    await event.wait()
            finally:
                self._backoff = None
            if self._stopping.is_set():
                return None
        raise AgentUnavailableError(failure)

    async def complete(self, delivery: Delivery) -> None:
        """Report the delivery to the agent as processed."""
        await self._report(delivery, "processed")

    async def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Report the delivery to the agent as released after the given delay."""
        await self._report(delivery, "released", delay_seconds)

    async def _report(self, delivery: Delivery, status: str, delay: int | None = None) -> None:
        """Send the delivery's outcome to the agent.

        Each delivery may report only one outcome, shared with the sync twin. Raises an
        ``AgentProtocolError`` when the agent rejects the result with a 4xx, and an
        ``AgentUnavailableError`` when it is unreachable or fails with a 5xx.
        """
        self._claims.claim(delivery)
        payload = _result_payload(delivery, status, delay)
        for attempt in range(3):
            try:
                code, _ = await self._request("POST", "/result", http_timeout=10, payload=payload)
            except _CONNECTION_ERRORS:
                if attempt < 2:
                    await anyio.sleep(0.1)
                continue
            _result_status(code)
            return
        raise AgentUnavailableError(_UNREACHABLE)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        http_timeout: float,
        payload: dict[str, str | int] | None = None,
    ) -> tuple[int, bytes]:
        """Send a request to the agent and return its status code and body.

        Only a successful poll's body is read, through ``aiter_bytes``, and it is bounded
        by the response size limit. Connection failures are raised as-is so callers can
        retry them.
        """
        try:
            async with self._client.stream(
                method, path, json=payload, timeout=http_timeout
            ) as response:
                # Only successful polls have a body we use. Never buffer error/result bodies.
                if method != "GET" or response.status_code != 200:
                    return response.status_code, b""
                _reject_encoding(response.headers.get("content-encoding", "identity"))
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    _take_chunk(body, chunk)
                return response.status_code, bytes(body)
        except _CONNECTION_ERRORS:
            raise
        except httpx.HTTPError:
            raise AgentUnavailableError("The agent HTTP request failed.") from None

    async def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Do nothing, since the agent owns the visibility heartbeats for its deliveries."""

    def interrupt(self) -> None:
        """Stop polling once the current ``GET /next`` finishes.

        The in-flight poll is never aborted, but a retry backoff is woken immediately.
        Result reporting stays available. This does not block and is safe to call from
        any thread. The sync twin stops polling too.
        """
        self._stopping.set()
        self._blocking.interrupt()
        loop = self._loop
        backoff = self._backoff
        if loop is None or backoff is None:
            return
        # A closed loop has no pending backoff to wake.
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(backoff.set)

    async def aclose(self) -> None:
        """Stop polling and close this HTTP client and the sync twin."""
        self.interrupt()
        await self._client.aclose()
        self._blocking.close()
