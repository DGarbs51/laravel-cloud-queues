"""Public exception hierarchy (PROJECT_SCOPE.md §24).

Every error has exactly one classification. The worker, CLI, eager mode and
transports all branch on :attr:`LaravelCloudQueuesError.classification`, never on
concrete AWS SDK or Redis client exceptions.

=================  ===========================================  ==========================
Classification     Examples                                     Worker behavior
=================  ===========================================  ==========================
``DISPATCH``       configuration, queue not found, too large,   raised to the caller;
                   invalid option, unserializable argument      nothing is sent
``JOB_DEFECT``     malformed envelope, unsupported version,     terminal on first delivery
                   unknown job, codec/argument mismatch,
                   ``@pointer`` overflow body
``HANDLER``        exception from the handler or its teardown   retry policy applies
``TRANSPORT``      transient receive error, agent 4xx on        logged; worker continues
                   ``/result``
``FATAL``          agent unhealthy, lost lease, ambiguous       worker stops with
                   acknowledgement, broker connection lost      :attr:`FatalWorkerError.exit_code`
=================  ===========================================  ==========================

Chained exceptions must never carry secrets: transports wrap client errors with a
sanitized message and ``raise ... from None`` when the original could contain
credentials or receipt handles.
"""

from __future__ import annotations

import enum
from typing import ClassVar

__all__ = [
    "AgentProtocolError",
    "AgentUnavailableError",
    "AmbiguousAcknowledgementError",
    "ArgumentError",
    "ArgumentMismatchError",
    "BrokerConnectionError",
    "CodecError",
    "ConfigurationError",
    "DispatchError",
    "ErrorClass",
    "FatalWorkerError",
    "InvalidQueueOptionError",
    "JobDefectError",
    "JobFailedError",
    "JobTimeoutError",
    "LaravelCloudQueuesError",
    "LeaseLostError",
    "MalformedEnvelopeError",
    "ManagedQueueNotFoundError",
    "MaxAttemptsExceededError",
    "PayloadTooLargeError",
    "SerializationError",
    "TransportError",
    "UnknownJobError",
    "UnsupportedEnvelopeVersionError",
    "UnsupportedOverflowPayloadError",
]


class ErrorClass(enum.Enum):
    """How the worker, CLI and eager mode treat an error."""

    DISPATCH = "dispatch"
    JOB_DEFECT = "job_defect"
    HANDLER = "handler"
    TRANSPORT = "transport"
    FATAL = "fatal"


class LaravelCloudQueuesError(Exception):
    """Base class for every package error."""

    classification: ClassVar[ErrorClass] = ErrorClass.HANDLER


# --- Dispatch errors: raised to the caller, nothing sent -------------------------------


class DispatchError(LaravelCloudQueuesError):
    classification = ErrorClass.DISPATCH


class ConfigurationError(DispatchError):
    """Invalid or ambiguous configuration. The worker exits 2 when raised at startup."""


class ManagedQueueNotFoundError(DispatchError):
    """The target queue does not exist (SQS ``AWS.SimpleQueueService.NonExistentQueue``)."""

    def __init__(self, queue: str) -> None:
        super().__init__(f"Queue [{queue}] does not exist.")
        self.queue = queue


class PayloadTooLargeError(DispatchError):
    """The fully encoded message body exceeds the transport limit (UTF-8 bytes)."""

    def __init__(self, *, size: int, limit: int, queue: str | None = None) -> None:
        where = f" for queue [{queue}]" if queue else ""
        super().__init__(f"Encoded job payload is {size} bytes{where}; the limit is {limit} bytes.")
        self.size = size
        self.limit = limit
        self.queue = queue


class InvalidQueueOptionError(DispatchError, ValueError):
    """Invalid dispatch option or option combination (delay, FIFO, fair-queue group)."""


class SerializationError(DispatchError):
    """An argument cannot be encoded by the registered codecs."""


class ArgumentError(DispatchError, TypeError):
    """Dispatch arguments do not bind to the job signature, or supply an injected parameter."""


# --- Deterministic job defects: terminal on first delivery -----------------------------


class JobDefectError(LaravelCloudQueuesError):
    classification = ErrorClass.JOB_DEFECT


class MalformedEnvelopeError(JobDefectError):
    """The message body is not a valid v1 envelope (bad JSON, limits, duplicate keys, shape)."""


class UnsupportedEnvelopeVersionError(JobDefectError):
    def __init__(self, version: object) -> None:
        super().__init__(f"Unsupported envelope version {version!r}.")
        self.version = version


class UnknownJobError(JobDefectError):
    """The job name is not registered. Never retried; never triggers an import."""

    def __init__(self, job_name: str) -> None:
        super().__init__(f"No job is registered under the name [{job_name}].")
        self.job_name = job_name


class CodecError(JobDefectError):
    """A received value cannot be decoded for its declared annotation."""


class ArgumentMismatchError(JobDefectError):
    """Decoded arguments are incompatible with the registered handler signature."""


class UnsupportedOverflowPayloadError(JobDefectError):
    """A Laravel overflow ``{"@pointer": ...}`` body; overflow is not supported in v1."""


# --- Terminal failure reasons recorded by the worker (exception in failed_job) ---------


class JobFailedError(LaravelCloudQueuesError):
    """Explicit terminal failure via ``JobContext.fail()``; never retried."""


class MaxAttemptsExceededError(LaravelCloudQueuesError):
    """Delivery attempt exceeds ``tries`` before running (Laravel MaxAttemptsExceededException)."""


class JobTimeoutError(LaravelCloudQueuesError):
    """The job exceeded its timeout (Laravel TimeoutExceededException)."""


# --- Transport conditions that do not stop the worker ----------------------------------


class TransportError(LaravelCloudQueuesError):
    """Transient broker/agent condition. Direct-mode receive errors sleep 1 s and retry."""

    classification = ErrorClass.TRANSPORT


class AgentProtocolError(TransportError):
    """The agent rejected an outcome with HTTP 4xx. Logged; the worker continues."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


# --- Fatal worker errors: stop the worker ----------------------------------------------


class FatalWorkerError(LaravelCloudQueuesError):
    classification = ErrorClass.FATAL
    exit_code: ClassVar[int] = 1


class AgentUnavailableError(FatalWorkerError):
    """Agent unreachable, 5xx, or invalid ``/next`` response. Exit 0 (Laravel parity, §11)."""

    exit_code = 0


class LeaseLostError(FatalWorkerError):
    """Visibility/reservation renewal failed: the worker no longer owns the message."""


class AmbiguousAcknowledgementError(FatalWorkerError):
    """The outcome may or may not have been applied; never report a second outcome."""


class BrokerConnectionError(FatalWorkerError):
    """Broker connection lost after bounded retries."""
