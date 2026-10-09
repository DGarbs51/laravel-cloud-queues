"""The Redis and Valkey queue transport, available with the optional ``[redis]`` extra.

Message bodies are opaque to the transport. Connection setup is retried three times
with a bounded backoff, but commands are sent only once, since retrying after an
uncertain reply could duplicate a push or reserve another job. Receive errors are
transient, while uncertain outcome reports stop the worker.
"""

from __future__ import annotations

import asyncio
import json
import math
import ssl
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from hashlib import sha256
from threading import Event
from typing import TYPE_CHECKING, Protocol, TypeVar, cast
from urllib.parse import urlsplit
from uuid import uuid4

import anyio

from ...config import RedisConfig
from ...errors import (
    AmbiguousAcknowledgementError,
    BrokerConnectionError,
    ConfigurationError,
    LeaseLostError,
    TransportError,
)
from ..base import Delivery, OutgoingMessage, SentMessage, json_object
from . import _scripts

if TYPE_CHECKING:
    from redis.asyncio.connection import AbstractConnection as AsyncConnection
    from redis.connection import Connection, ConnectionPool


class _Connection(Protocol):
    """The redis-py connection methods used here, which redis-py leaves untyped."""

    def send_command(self, *args: str | float) -> None:
        """Write a single command to the socket."""
        ...

    def read_response(self) -> object:
        """Read the reply to the last command."""
        ...

    def disconnect(self) -> None:
        """Close the socket."""
        ...


class _Pool(Protocol):
    """The redis-py connection pool members used here, which redis-py types only partially."""

    @property
    def connection_kwargs(self) -> dict[str, object]:
        """Get the keyword arguments passed to every new connection."""
        ...

    def get_connection(self, command_name: str | None = None) -> Connection:
        """Acquire a connected connection, connecting on first use."""
        ...

    def release(self, connection: Connection) -> None:
        """Return a connection to the pool."""
        ...

    def disconnect(self) -> None:
        """Close every pooled connection."""
        ...


class _AsyncConnection(Protocol):
    """The asynchronous Redis connection methods used here."""

    async def send_command(self, *args: str | float) -> None:
        """Write a single command to the socket."""
        ...

    async def read_response(self) -> object:
        """Read the reply to the last command."""
        ...

    async def disconnect(self) -> None:
        """Close the socket."""
        ...


class _AsyncPool(Protocol):
    """The asynchronous Redis pool methods used here."""

    async def get_connection(self, command_name: str | None = None) -> AsyncConnection:
        """Acquire a connected connection, connecting on first use."""
        ...

    async def release(self, connection: AsyncConnection) -> None:
        """Return a connection to the pool."""
        ...

    async def disconnect(self) -> None:
        """Close every pooled connection."""
        ...


class _PoolOptions(Protocol):
    """The settings shared by synchronous and asynchronous pools."""

    @property
    def connection_kwargs(self) -> dict[str, object]:
        """Get the keyword arguments passed to every new connection."""
        ...


_PoolT = TypeVar("_PoolT", bound=_PoolOptions)


def _utf8(member: str) -> bool:
    """Determine if the member survives UTF-8 encoding, unlike one with lone surrogates."""
    try:
        member.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


class _RedisBase:
    """The connection handling shared by the Redis producer and consumer."""

    def __init__(self, config: RedisConfig) -> None:
        """Create a new Redis transport instance.

        Raises a ``ConfigurationError`` if the ``[redis]`` extra is missing, or if the URL
        is invalid or attempts to weaken TLS verification.
        """
        try:
            import redis
        except ImportError:
            raise ConfigurationError(
                'Redis requires the optional dependency: pip install "laravel-cloud-queues[redis]"'
            ) from None

        self._redis = redis
        self._prefix = config.prefix
        # get_connection() stopped taking the command name in redis-py 5.3.
        self._legacy_pool = tuple(int(part) for part in redis.__version__.split(".")[:2]) < (5, 3)
        self._config = config

    def _pools(self, factory: Callable[[str], _PoolT], retry: object) -> tuple[_PoolT, _PoolT]:
        """Build polling and reporting pools with bounded I/O and verified TLS."""
        scheme = urlsplit(self._config.url).scheme

        def pool_from_url(socket_timeout: int) -> _PoolT:
            """Build a pool while enforcing safe URL options."""
            pool = factory(self._config.url)
            options = pool.connection_kwargs
            if scheme == "rediss":
                if options.get("ssl_cert_reqs", "required") not in {"required", ssl.CERT_REQUIRED}:
                    raise ValueError
                if not options.get("ssl_check_hostname", True):
                    raise ValueError
                options.update(ssl_cert_reqs="required", ssl_check_hostname=True)
            # URL options must not disable bounds, decoding or safe retry semantics.
            options.update(
                decode_responses=True,
                encoding="utf-8",
                encoding_errors="surrogateescape",
                socket_connect_timeout=2,
                socket_timeout=socket_timeout,
                retry=retry,
                retry_on_error=[],
                retry_on_timeout=False,
            )
            return pool

        try:
            if scheme not in {"redis", "rediss"}:
                raise ValueError
            return pool_from_url(2), pool_from_url(10)
        except (ValueError, TypeError, self._redis.RedisError):
            raise ConfigurationError("Invalid Redis URL or TLS verification settings.") from None

    def _keys(self, queue: str) -> tuple[str, str, str, str]:
        """Get the pending, delayed, reserved and notify keys for the given queue."""
        # A pending key must never alias another queue's internal key.
        if queue.endswith((":delayed", ":reserved", ":notify")):
            raise ConfigurationError(
                "Redis queue names must not end with :delayed, :reserved, or :notify."
            )
        pending = f"{self._prefix}queues:{queue}"
        return pending, f"{pending}:delayed", f"{pending}:reserved", f"{pending}:notify"

    def _command_error(
        self, exc: Exception, reporting: bool
    ) -> TransportError | ConfigurationError | AmbiguousAcknowledgementError:
        """Classify a failed exchange without exposing credentials or receipts."""
        if isinstance(
            exc,
            (self._redis.AuthenticationError, self._redis.AuthenticationWrongNumberOfArgsError),
        ) or (
            isinstance(exc, self._redis.ResponseError)
            and str(exc).split(" ", 1)[0] in {"NOAUTH", "WRONGPASS"}
        ):
            return ConfigurationError("Redis authentication failed.")
        outcome_error = AmbiguousAcknowledgementError if reporting else TransportError
        return outcome_error("Redis command failed; its outcome may be unknown.")

    def _report_args(
        self, script: str, delivery: Delivery, seconds: int
    ) -> tuple[str | float, ...]:
        """Build an outcome command for a delivery with a reservation."""
        if delivery.receipt is None:
            raise LeaseLostError("Redis delivery has no reservation.")
        return "EVAL", script, 4, *self._keys(delivery.queue), delivery.receipt, seconds


class _RedisTransport(_RedisBase):
    """The synchronous Redis connection handling."""

    def __init__(self, config: RedisConfig) -> None:
        """Create a new synchronous Redis transport instance."""
        super().__init__(config)
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        self._pool: _Pool
        self._reporting_pool: _Pool
        # Narrow redis-py's untyped keyword arguments to the URL-only call used here.
        factory = cast(
            "Callable[[str], ConnectionPool]",
            self._redis.ConnectionPool.from_url,  # pyright: ignore[reportUnknownMemberType]
        )
        self._pool, self._reporting_pool = self._pools(factory, Retry(NoBackoff(), 0))

    def _command(self, *args: str | float, reporting: bool = False) -> object:
        """Send a single command to Redis and return its reply.

        Only acquiring a connection is retried; the command itself is sent once. Failures
        raise a ``TransportError``, or a ``BrokerConnectionError`` or
        ``AmbiguousAcknowledgementError`` when ``reporting`` a delivery outcome.
        """
        pool = self._reporting_pool if reporting else self._pool
        # Acquire/connect before sending so only definitely-unsent commands are retried.
        for attempt in range(3):
            try:
                if self._legacy_pool:
                    connection = pool.get_connection(str(args[0]))
                else:
                    connection = pool.get_connection()
                break
            except self._redis.AuthenticationError:
                raise ConfigurationError("Redis authentication failed.") from None
            except (self._redis.ConnectionError, self._redis.TimeoutError):
                if attempt < 2:
                    time.sleep(0.05 * 2**attempt)
            except (ValueError, TypeError, self._redis.RedisError):
                raise ConfigurationError("Invalid Redis connection settings.") from None
        else:
            error = BrokerConnectionError if reporting else TransportError
            raise error("Redis connection unavailable after bounded retries.")
        try:
            return self._exchange(connection, args, reporting)
        finally:
            pool.release(connection)

    def _exchange(
        self, connection: _Connection, args: tuple[str | float, ...], reporting: bool
    ) -> object:
        """Send the command over the acquired connection and return its reply."""
        try:
            connection.send_command(*args)
            return connection.read_response()
        except (self._redis.RedisError, UnicodeError) as exc:
            # A partial write or lost reply is ambiguous. Do not replay mutations.
            connection.disconnect()
            raise self._command_error(exc, reporting) from None

    def close(self) -> None:
        """Disconnect every pooled connection."""
        self._pool.disconnect()
        self._reporting_pool.disconnect()


class RedisProducer(_RedisTransport):
    """A producer that pushes messages onto Redis queues."""

    @property
    def max_payload_bytes(self) -> int | None:
        """Get the byte limit for the encoded body, which Redis does not impose."""
        return None

    @property
    def supports_fifo(self) -> bool:
        """Determine if the transport supports FIFO and fair-queue options."""
        return False

    def send(self, message: OutgoingMessage) -> SentMessage:
        """Push the message onto the queue, or onto the delayed set when it has a delay."""
        message_id, wrapper = _message(message)
        self._command(
            "EVAL", _scripts.SEND, 4, *self._keys(message.queue), wrapper, message.delay_seconds
        )
        return SentMessage(message_id=message_id, queue=message.queue)


class RedisConsumer(_RedisTransport):
    """A consumer that reserves jobs from prioritized Redis queues.

    Each receive reserves at most one job, and the watchdog renews reservations over
    a separate pool. ``BLPOP`` waits in slices of at most one second so that interrupts
    and due jobs are noticed promptly. Reservation and delay times use the Redis clock,
    never the client's wall clock.
    """

    def __init__(self, config: RedisConfig, *, lease_seconds: int = 60) -> None:
        """Create a new Redis consumer instance.

        Raises a ``ConfigurationError`` if ``lease_seconds`` is not positive.
        """
        if lease_seconds <= 0:
            raise ConfigurationError("Redis lease_seconds must be positive.")
        super().__init__(config)
        self._lease_seconds = lease_seconds
        self._interrupted = Event()

    @property
    def supports_renewal(self) -> bool:
        """Determine if the watchdog should renew the lease on deliveries."""
        return True

    def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Reserve the next job from the first queue with work, waiting up to ``wait_seconds``.

        Malformed jobs are still returned, with their raw member as the receipt, so that
        core can fail and delete them.
        """
        if not math.isfinite(wait_seconds) or wait_seconds < 0:
            raise ValueError("wait_seconds must be finite and nonnegative.")
        if not queues:
            return None
        deadline = time.monotonic() + wait_seconds
        notify = [self._keys(queue)[3] for queue in queues]
        while not self._interrupted.is_set():
            for queue in queues:
                reply = self._command(
                    "EVAL", _scripts.RESERVE, 4, *self._keys(queue), self._lease_seconds
                )
                if reply is None:
                    continue
                return _delivery(reply, queue)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted.is_set():
                return None
            self._command("BLPOP", *notify, min(remaining, 1.0))
        return None

    def _report(self, script: str, delivery: Delivery, seconds: int = 0) -> None:
        """Run an outcome script against the delivery's reservation.

        Raises a ``LeaseLostError`` if the reservation is missing or has expired.
        """
        owned = self._command(*self._report_args(script, delivery, seconds), reporting=True)
        if owned != 1:
            raise LeaseLostError("Redis reservation is missing or expired.")

    def complete(self, delivery: Delivery) -> None:
        """Delete the reserved job from the queue."""
        self._report(_scripts.COMPLETE, delivery)

    def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Release the reserved job back onto the queue after the given delay."""
        self._report(_scripts.RELEASE, delivery, delay_seconds)

    def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Extend the job's reservation to now plus ``lease_seconds``."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive.")
        self._report(_scripts.RENEW, delivery, lease_seconds)

    def interrupt(self) -> None:
        """Make a waiting :meth:`receive` return early."""
        self._interrupted.set()

    def close(self) -> None:
        """Interrupt any waiting receive and disconnect from Redis."""
        self.interrupt()
        super().close()


def _message(message: OutgoingMessage) -> tuple[str, str]:
    """Encode the opaque body in the shared Redis wrapper."""
    message_id = str(uuid4())
    wrapper = json.dumps(
        {"id": message_id, "attempts": 0, "body": message.body}, separators=(",", ":")
    )
    return message_id, wrapper


def _delivery(reply: object, queue: str) -> Delivery:
    """Parse a reservation while preserving malformed members for terminal failure."""
    if isinstance(reply, str) and _utf8(reply):
        member = reply
        # RESERVE validated the wrapper before re-encoding it with cjson.
        job: object = json.loads(member)
        fields: Mapping[str, object] = job if json_object(job) else {}
        message_id, body, attempts = (
            fields.get("id"),
            fields.get("body"),
            fields.get("attempts"),
        )
        if not (
            isinstance(message_id, str)
            and isinstance(body, str)
            and isinstance(attempts, (int, float))
        ):
            raise TransportError("Invalid Redis reservation response.")
    else:
        # Malformed wrappers arrive as a one-element array, or as a member that
        # is not valid UTF-8. They keep their raw receipt so core can fail and
        # delete them.
        match reply:
            case str():
                member = reply
            case [str() as member]:
                pass
            case _:
                raise TransportError("Invalid Redis reservation response.")
        digest = sha256(member.encode("utf-8", errors="surrogateescape")).hexdigest()
        message_id, body, attempts = f"malformed-{digest}", member, 1
    return Delivery(
        message_id=message_id,
        queue=queue,
        body=body,
        attempt=int(attempts),
        receipt=member,
        received_at=time.monotonic(),
    )


class _AsyncRedisTransport(_RedisBase):
    """The asynchronous Redis connection handling."""

    def __init__(self, config: RedisConfig) -> None:
        """Create a new asynchronous Redis transport instance."""
        super().__init__(config)
        from redis.asyncio import ConnectionPool as AsyncConnectionPool
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        self._pool: _AsyncPool
        self._reporting_pool: _AsyncPool
        factory = cast(
            "Callable[[str], AsyncConnectionPool]",
            AsyncConnectionPool.from_url,  # pyright: ignore[reportUnknownMemberType]
        )
        self._pool, self._reporting_pool = self._pools(factory, Retry(NoBackoff(), 0))

    async def _command(self, *args: str | float, reporting: bool = False) -> object:
        """Send a command once, retrying only connection acquisition."""
        pool = self._reporting_pool if reporting else self._pool
        for attempt in range(3):
            try:
                if self._legacy_pool:
                    connection = await pool.get_connection(str(args[0]))
                else:
                    connection = await pool.get_connection()
                break
            except self._redis.AuthenticationError:
                raise ConfigurationError("Redis authentication failed.") from None
            except (self._redis.ConnectionError, self._redis.TimeoutError):
                if attempt < 2:
                    await anyio.sleep(0.05 * 2**attempt)
            except (ValueError, TypeError, self._redis.RedisError):
                raise ConfigurationError("Invalid Redis connection settings.") from None
        else:
            error = BrokerConnectionError if reporting else TransportError
            raise error("Redis connection unavailable after bounded retries.")
        try:
            return await self._exchange(connection, args, reporting)
        finally:
            await pool.release(connection)

    async def _exchange(
        self, connection: _AsyncConnection, args: tuple[str | float, ...], reporting: bool
    ) -> object:
        """Send the command and discard connections with uncertain replies."""
        try:
            await connection.send_command(*args)
            return await connection.read_response()
        except asyncio.CancelledError:
            # A cancelled BLPOP reply must never become another command's reply.
            await connection.disconnect()
            raise
        except (self._redis.RedisError, UnicodeError) as exc:
            await connection.disconnect()
            raise self._command_error(exc, reporting) from None

    async def aclose(self) -> None:
        """Disconnect every asynchronous pooled connection."""
        await self._pool.disconnect()
        await self._reporting_pool.disconnect()


class AsyncRedisProducer(_AsyncRedisTransport):
    """A producer that pushes messages onto Redis queues from an asyncio loop."""

    @property
    def max_payload_bytes(self) -> int | None:
        """Get the byte limit for the encoded body, which Redis does not impose."""
        return None

    @property
    def supports_fifo(self) -> bool:
        """Determine if the transport supports FIFO and fair-queue options."""
        return False

    async def send(self, message: OutgoingMessage) -> SentMessage:
        """Push the message onto the queue, or onto the delayed set when delayed."""
        message_id, wrapper = _message(message)
        await self._command(
            "EVAL", _scripts.SEND, 4, *self._keys(message.queue), wrapper, message.delay_seconds
        )
        return SentMessage(message_id=message_id, queue=message.queue)


class AsyncRedisConsumer(_AsyncRedisTransport):
    """A consumer that reserves jobs from Redis without blocking the asyncio loop.

    Interrupts cancel only the notification wait, never a reservation whose reply could
    be lost. The synchronous twin uses separate pools for watchdog and signal reporting.
    """

    def __init__(self, config: RedisConfig, *, lease_seconds: int = 60) -> None:
        """Create a new asynchronous Redis consumer instance."""
        if lease_seconds <= 0:
            raise ConfigurationError("Redis lease_seconds must be positive.")
        super().__init__(config)
        self._lease_seconds = lease_seconds
        self._interrupted = Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wait: asyncio.Task[object] | None = None
        self._blocking: RedisConsumer = RedisConsumer(config, lease_seconds=lease_seconds)

    @property
    def supports_renewal(self) -> bool:
        """Determine if the worker should renew the lease on deliveries."""
        return True

    @property
    def blocking(self) -> RedisConsumer:
        """Get the synchronous twin for watchdog and signal reporting."""
        return self._blocking

    async def receive(self, queues: Sequence[str], wait_seconds: float) -> Delivery | None:
        """Reserve the next job from prioritized queues, waiting up to the deadline."""
        if not math.isfinite(wait_seconds) or wait_seconds < 0:
            raise ValueError("wait_seconds must be finite and nonnegative.")
        if not queues:
            return None
        self._loop = asyncio.get_running_loop()
        deadline = time.monotonic() + wait_seconds
        notify = [self._keys(queue)[3] for queue in queues]
        while not self._interrupted.is_set():
            for queue in queues:
                reply = await self._command(
                    "EVAL", _scripts.RESERVE, 4, *self._keys(queue), self._lease_seconds
                )
                if reply is None:
                    continue
                return _delivery(reply, queue)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted.is_set():
                return None
            self._wait = asyncio.create_task(self._command("BLPOP", *notify, min(remaining, 1.0)))
            try:
                # An interrupt can arrive from another thread before the task is stored.
                if self._interrupted.is_set():
                    self._wait.cancel()
                await self._wait
            except asyncio.CancelledError:
                if not self._interrupted.is_set():
                    raise
                return None
            finally:
                self._wait = None
        return None

    async def _report(self, script: str, delivery: Delivery, seconds: int = 0) -> None:
        """Run an outcome script while checking ownership of the reservation."""
        owned = await self._command(*self._report_args(script, delivery, seconds), reporting=True)
        if owned != 1:
            raise LeaseLostError("Redis reservation is missing or expired.")

    async def complete(self, delivery: Delivery) -> None:
        """Delete the reserved job from the queue."""
        await self._report(_scripts.COMPLETE, delivery)

    async def release(self, delivery: Delivery, delay_seconds: int) -> None:
        """Release the reserved job back onto the queue after the given delay."""
        await self._report(_scripts.RELEASE, delivery, delay_seconds)

    async def renew(self, delivery: Delivery, lease_seconds: int) -> None:
        """Extend the job's reservation to now plus the lease."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive.")
        await self._report(_scripts.RENEW, delivery, lease_seconds)

    def _cancel_wait(self) -> None:
        """Cancel only the currently pending notification wait on its loop."""
        if self._wait is not None:
            self._wait.cancel()

    def interrupt(self) -> None:
        """Wake a pending receive without cancelling an in-flight reservation."""
        self._interrupted.set()
        if self._loop is not None:
            # A closed loop has no pending notification wait to interrupt.
            with suppress(RuntimeError):
                self._loop.call_soon_threadsafe(self._cancel_wait)

    async def aclose(self) -> None:
        """Interrupt receive and disconnect both asynchronous pools and the twin."""
        self.interrupt()
        await super().aclose()
        self._blocking.close()
