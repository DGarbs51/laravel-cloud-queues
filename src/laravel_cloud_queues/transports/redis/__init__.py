"""The Redis and Valkey queue transport, available with the optional ``[redis]`` extra.

Message bodies are opaque to the transport. Connection setup is retried three times
with a bounded backoff, but commands are sent only once, since retrying after an
uncertain reply could duplicate a push or reserve another job. Receive errors are
transient, while uncertain outcome reports stop the worker.
"""

from __future__ import annotations

import json
import math
import ssl
import time
from collections.abc import Mapping, Sequence
from hashlib import sha256
from threading import Event
from typing import TYPE_CHECKING, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

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


def _utf8(member: str) -> bool:
    """Determine if the member survives UTF-8 encoding, unlike one with lone surrogates."""
    try:
        member.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


class _RedisTransport:
    """The connection handling shared by the Redis producer and consumer."""

    def __init__(self, config: RedisConfig) -> None:
        """Create a new Redis transport instance.

        Raises a ``ConfigurationError`` if the ``[redis]`` extra is missing, or if the URL
        is invalid or attempts to weaken TLS verification.
        """
        try:
            import redis
            from redis.backoff import NoBackoff
            from redis.retry import Retry
        except ImportError:
            raise ConfigurationError(
                'Redis requires the optional dependency: pip install "laravel-cloud-queues[redis]"'
            ) from None

        self._redis = redis
        self._prefix = config.prefix
        # get_connection() stopped taking the command name in redis-py 5.3.
        self._legacy_pool = tuple(int(part) for part in redis.__version__.split(".")[:2]) < (5, 3)
        scheme = urlsplit(config.url).scheme

        def pool_from_url(socket_timeout: int) -> ConnectionPool:
            """Build a pool from the URL, enforcing TLS verification and bounded I/O."""
            # Type-checker ignore, kept deliberately. redis-py declares
            # `ConnectionPool.from_url(cls, url: str, **kwargs)` without annotating
            # **kwargs, so pyright strict reports the whole method as partially unknown
            # even though no keyword arguments are passed here; mypy and ty accept it.
            # The alternatives are worse: ConnectionPool(...) has the same untyped
            # **kwargs, and parsing the URL ourselves would duplicate redis-py's TLS, db
            # and credential handling. Remove the ignore once redis-py annotates **kwargs
            # (pyright's reportUnnecessaryTypeIgnoreComment will then flag it).
            pool = redis.ConnectionPool.from_url(config.url)  # pyright: ignore[reportUnknownMemberType]
            options = pool.connection_kwargs
            if scheme == "rediss":
                if options.get("ssl_cert_reqs", "required") not in {"required", ssl.CERT_REQUIRED}:
                    raise ValueError
                if not options.get("ssl_check_hostname", True):
                    raise ValueError
                options.update(ssl_cert_reqs="required", ssl_check_hostname=True)
            # URL options must not disable bounds, decoding or safe retry semantics.
            # CA/client certificate URL options are otherwise left to redis-py.
            options.update(
                decode_responses=True,
                encoding="utf-8",
                encoding_errors="surrogateescape",
                socket_connect_timeout=2,
                socket_timeout=socket_timeout,
                retry=Retry(NoBackoff(), 0),
                retry_on_error=[],
                retry_on_timeout=False,
            )
            return pool

        try:
            if scheme not in {"redis", "rediss"}:
                raise ValueError
            self._pool: _Pool = pool_from_url(socket_timeout=2)
            # Reporting gets its own pool so watchdog I/O cannot alter polling timeouts.
            self._reporting_pool: _Pool = pool_from_url(socket_timeout=10)
        except (ValueError, TypeError, redis.RedisError):
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
            if isinstance(
                exc,
                (self._redis.AuthenticationError, self._redis.AuthenticationWrongNumberOfArgsError),
            ) or (
                isinstance(exc, self._redis.ResponseError)
                and str(exc).split(" ", 1)[0] in {"NOAUTH", "WRONGPASS"}
            ):
                raise ConfigurationError("Redis authentication failed.") from None
            outcome_error = AmbiguousAcknowledgementError if reporting else TransportError
            raise outcome_error("Redis command failed; its outcome may be unknown.") from None

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
        message_id = str(uuid4())
        wrapper = json.dumps(
            {"id": message_id, "attempts": 0, "body": message.body}, separators=(",", ":")
        )
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
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._interrupted.is_set():
                return None
            self._command("BLPOP", *notify, min(remaining, 1.0))
        return None

    def _report(self, script: str, delivery: Delivery, seconds: int = 0) -> None:
        """Run an outcome script against the delivery's reservation.

        Raises a ``LeaseLostError`` if the reservation is missing or has expired.
        """
        if delivery.receipt is None:
            raise LeaseLostError("Redis delivery has no reservation.")
        owned = self._command(
            "EVAL",
            script,
            4,
            *self._keys(delivery.queue),
            delivery.receipt,
            seconds,
            reporting=True,
        )
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
