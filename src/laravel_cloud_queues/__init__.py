"""Laravel Cloud queues for Python applications."""

from __future__ import annotations

import importlib.metadata

from .config import QueueConfig, load_config
from .errors import (
    ArgumentError,
    ConfigurationError,
    DispatchError,
    InvalidQueueOptionError,
    JobDefectError,
    LaravelCloudQueuesError,
    ManagedQueueNotFoundError,
    PayloadTooLargeError,
    SerializationError,
)
from .jobs import DispatchReceipt, Job, JobContext, RetryPolicy, current_job
from .registry import Registry

# Single source of truth is pyproject.toml; bump with `uv version --bump <part>`.
__version__ = importlib.metadata.version("laravel-cloud-queues")
"""The installed version of the package."""

__all__ = [
    "ArgumentError",
    "ConfigurationError",
    "DispatchError",
    "DispatchReceipt",
    "InvalidQueueOptionError",
    "Job",
    "JobContext",
    "JobDefectError",
    "LaravelCloudQueuesError",
    "ManagedQueueNotFoundError",
    "PayloadTooLargeError",
    "QueueConfig",
    "Registry",
    "RetryPolicy",
    "SerializationError",
    "__version__",
    "current_job",
    "load_config",
]
