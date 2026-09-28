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
from collections.abc import Sequence
from hashlib import sha256
from threading import Event
from typing import Any
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
from ..base import Delivery, OutgoingMessage, SentMessage
from . import _scripts


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
        try:
            scheme = urlsplit(config.url).scheme
            if scheme not in {"redis", "rediss"}:
                raise ValueError
            self._pool = redis.ConnectionPool.from_url(config.url)
            options = self._pool.connection_kwargs
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
                socket_timeout=2,
                retry=Retry(NoBackoff(), 0),
                retry_on_error=[],
                retry_on_timeout=False,
            )
            reporting_options: dict[str, Any] = {**options, "socket_timeout": 10}
            self._reporting_pool = redis.ConnectionPool(
                connection_class=self._pool.connection_class, **reporting_options
            )
        except (ValueError, TypeError, redis.RedisError):
            raise ConfigurationError("Invalid Redis URL or TLS verification settings.") from None

    def _keys(self, queue: str) -> tuple[str, str, str, str]:
        """Get the pending, delayed, reserved and notify keys for the given queue."""
        pending = f"{self._prefix}queues:{queue}"
        return pending, f"{pending}:delayed", f"{pending}:reserved", f"{pending}:notify"

    def _command(self, *args: str | float, reporting: bool = False) -> object:
        """Send a single command to Redis and return its reply.

        Only acquiring a connection is retried; the command itself is sent once. Failures
        raise a ``TransportError``, or a ``BrokerConnectionError`` or
        ``AmbiguousAcknowledgementError`` when ``reporting`` a delivery outcome.
        """
        # Reporting gets its own pool so watchdog I/O cannot alter polling timeouts.
        pool = self._reporting_pool if reporting else self._pool
        # Acquire/connect before sending so only definitely-unsent commands are retried.
        for attempt in range(3):
            try:
                if self._redis.VERSION < (5, 3):
                    connection = pool.get_connection(str(args[0]))
                else:
                    connection = pool.get_connection()
                break
            except self._redis.AuthenticationError:
                raise ConfigurationError("Redis authentication failed.") from None
            except (self._redis.ConnectionError, self._redis.TimeoutError):
                if attempt == 2:
                    error = BrokerConnectionError if reporting else TransportError
                    raise error("Redis connection unavailable after bounded retries.") from None
                time.sleep(0.05 * 2**attempt)
            except (ValueError, TypeError, self._redis.RedisError):
                raise ConfigurationError("Invalid Redis connection settings.") from None
        else:
            raise AssertionError("unreachable: the last attempt breaks or raises")
        try:
            connection.send_command(*args)
            return connection.read_response()
        except (self._redis.RedisError, UnicodeError) as exc:
            # A partial write or lost reply is ambiguous. Do not replay mutations.
            connection.disconnect()
            if isinstance(
                exc,
                (
                    self._redis.AuthenticationError,
                    self._redis.exceptions.AuthenticationWrongNumberOfArgsError,
                ),
            ) or (
                isinstance(exc, self._redis.ResponseError)
                and str(exc).split(" ", 1)[0] in {"NOAUTH", "WRONGPASS"}
            ):
                raise ConfigurationError("Redis authentication failed.") from None
            outcome_error = AmbiguousAcknowledgementError if reporting else TransportError
            raise outcome_error("Redis command failed; its outcome may be unknown.") from None
        finally:
            pool.release(connection)

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
                member = self._command(
                    "EVAL", _scripts.RESERVE, 4, *self._keys(queue), self._lease_seconds
                )
                if member is not None:
                    if isinstance(member, str):
                        try:
                            member.encode("utf-8")
                        except UnicodeEncodeError:
                            # Preserve all bytes as the opaque receipt; core rejects the body.
                            member = [member]
                    wrapper: dict[str, Any]
                    if isinstance(member, str):
                        wrapper = json.loads(member)
                    elif (
                        isinstance(member, list) and len(member) == 1 and isinstance(member[0], str)
                    ):
                        # Malformed wrappers retain their raw receipt so core can fail/delete them.
                        member = member[0]
                        wrapper = {
                            "id": "malformed-"
                            + sha256(member.encode("utf-8", errors="surrogateescape")).hexdigest(),
                            "body": member,
                            "attempts": 1,
                        }
                    else:
                        raise TransportError("Invalid Redis reservation response.")
                    return Delivery(
                        message_id=wrapper["id"],
                        queue=queue,
                        body=wrapper["body"],
                        attempt=int(wrapper["attempts"]),
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
