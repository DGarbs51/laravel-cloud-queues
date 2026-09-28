"""The exceptions thrown by the queue package.

Every exception has exactly one classification. The worker, the CLI, eager mode and
the transports all branch on :attr:`LaravelCloudQueuesError.classification`, never on
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

Chained exceptions must never carry secrets. Transports wrap client errors in a
sanitized message and ``raise ... from None`` whenever the original could contain
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
    """The classification that decides how the worker, CLI and eager mode treat an error."""

    DISPATCH = "dispatch"
    """The error is raised to the caller and nothing is sent."""
    JOB_DEFECT = "job_defect"
    """The message can never run, so it fails terminally on its first delivery."""
    HANDLER = "handler"
    """The job's handler failed, so the job's retry policy applies."""
    TRANSPORT = "transport"
    """The broker or agent had a transient problem, so the worker logs it and continues."""
    FATAL = "fatal"
    """The worker can no longer run safely and must stop."""


class LaravelCloudQueuesError(Exception):
    """The base exception for every error thrown by the package."""

    classification: ClassVar[ErrorClass] = ErrorClass.HANDLER
    """The classification that decides how the error is handled."""


# --- Dispatch errors: raised to the caller, nothing sent -------------------------------


class DispatchError(LaravelCloudQueuesError):
    """The exception thrown when a job cannot be dispatched; nothing is sent."""

    classification = ErrorClass.DISPATCH
    """The classification that decides how the error is handled."""


class ConfigurationError(DispatchError):
    """The exception thrown when the configuration is invalid or ambiguous.

    The worker exits with status 2 when this is raised at startup.
    """


class ManagedQueueNotFoundError(DispatchError):
    """The exception thrown when the target queue does not exist.

    This corresponds to the SQS ``AWS.SimpleQueueService.NonExistentQueue`` error.
    """

    def __init__(self, queue: str) -> None:
        """Create a new exception instance."""
        super().__init__(f"Queue [{queue}] does not exist.")
        self.queue = queue
        """The name of the queue that does not exist."""


class PayloadTooLargeError(DispatchError):
    """The exception thrown when the encoded message body exceeds the transport limit.

    Both sizes are measured in UTF-8 bytes of the fully encoded body.
    """

    def __init__(self, *, size: int, limit: int, queue: str | None = None) -> None:
        """Create a new exception instance."""
        where = f" for queue [{queue}]" if queue else ""
        super().__init__(f"Encoded job payload is {size} bytes{where}; the limit is {limit} bytes.")
        self.size = size
        """The size of the encoded payload in bytes."""
        self.limit = limit
        """The maximum payload size allowed by the transport in bytes."""
        self.queue = queue
        """The name of the target queue, if known."""


class InvalidQueueOptionError(DispatchError, ValueError):
    """The exception thrown when a dispatch option or combination of options is invalid.

    This covers the delay, FIFO and fair-queue group options.
    """


class SerializationError(DispatchError):
    """The exception thrown when an argument cannot be encoded by the registered codecs."""


class ArgumentError(DispatchError, TypeError):
    """The exception thrown when the dispatch arguments do not match the job signature.

    This is also raised when the arguments supply a parameter that the worker injects.
    """


# --- Deterministic job defects: terminal on first delivery -----------------------------


class JobDefectError(LaravelCloudQueuesError):
    """The exception thrown when a message can never run; it fails on its first delivery."""

    classification = ErrorClass.JOB_DEFECT
    """The classification that decides how the error is handled."""


class MalformedEnvelopeError(JobDefectError):
    """The exception thrown when the message body is not a valid v1 envelope.

    This covers invalid JSON, exceeded limits, duplicate keys and an unexpected shape.
    """


class UnsupportedEnvelopeVersionError(JobDefectError):
    """The exception thrown when the message envelope has an unsupported version."""

    def __init__(self, version: object) -> None:
        """Create a new exception instance."""
        super().__init__(f"Unsupported envelope version {version!r}.")
        self.version = version
        """The version found in the envelope."""


class UnknownJobError(JobDefectError):
    """The exception thrown when no job is registered under the message's job name.

    The message is never retried, and the lookup never triggers an import.
    """

    def __init__(self, job_name: str) -> None:
        """Create a new exception instance."""
        super().__init__(f"No job is registered under the name [{job_name}].")
        self.job_name = job_name
        """The unregistered job name."""


class CodecError(JobDefectError):
    """The exception thrown when a received value cannot be decoded for its annotation."""


class ArgumentMismatchError(JobDefectError):
    """The exception thrown when the decoded arguments do not match the handler signature."""


class UnsupportedOverflowPayloadError(JobDefectError):
    """The exception thrown when a message has a Laravel overflow ``{"@pointer": ...}`` body.

    Overflow payloads are not supported in v1.
    """


# --- Terminal failure reasons recorded by the worker (exception in failed_job) ---------


class JobFailedError(LaravelCloudQueuesError):
    """The exception recorded when a job is explicitly failed with ``JobContext.fail()``.

    The job is never retried.
    """


class MaxAttemptsExceededError(LaravelCloudQueuesError):
    """The exception recorded when a delivery exceeds the job's ``tries`` before it runs.

    This mirrors Laravel's ``MaxAttemptsExceededException``.
    """


class JobTimeoutError(LaravelCloudQueuesError):
    """The exception recorded when a job exceeds its timeout.

    This mirrors Laravel's ``TimeoutExceededException``.
    """


# --- Transport conditions that do not stop the worker ----------------------------------


class TransportError(LaravelCloudQueuesError):
    """The exception thrown when the broker or agent has a transient problem.

    In direct mode, the worker sleeps for one second after a failed receive and retries.
    """

    classification = ErrorClass.TRANSPORT
    """The classification that decides how the error is handled."""


class AgentProtocolError(TransportError):
    """The exception thrown when the agent rejects an outcome with an HTTP 4xx response.

    The worker logs the rejection and continues.
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        """Create a new exception instance."""
        super().__init__(message)
        self.status = status
        """The HTTP status code returned by the agent, if any."""


# --- Fatal worker errors: stop the worker ----------------------------------------------


class FatalWorkerError(LaravelCloudQueuesError):
    """The exception thrown when the worker can no longer run safely and must stop."""

    classification = ErrorClass.FATAL
    """The classification that decides how the error is handled."""
    exit_code: ClassVar[int] = 1
    """The process exit code the worker stops with."""


class AgentUnavailableError(FatalWorkerError):
    """The exception thrown when the agent is unreachable or unhealthy.

    This covers connection failures, HTTP 5xx responses and an invalid ``/next`` response.
    The worker exits with status 0, matching Laravel.
    """

    exit_code = 0
    """The process exit code the worker stops with."""


class LeaseLostError(FatalWorkerError):
    """The exception thrown when the worker no longer owns the message it is running.

    This is raised when renewing the message's visibility or reservation fails.
    """


class AmbiguousAcknowledgementError(FatalWorkerError):
    """The exception thrown when an outcome may or may not have been applied.

    A second outcome must never be reported for the message.
    """


class BrokerConnectionError(FatalWorkerError):
    """The exception thrown when the broker connection is lost after bounded retries."""
