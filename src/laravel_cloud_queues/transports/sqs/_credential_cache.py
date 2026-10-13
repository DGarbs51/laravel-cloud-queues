"""Share refreshable AWS credentials across the processes on one host."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import time
from datetime import UTC, datetime

from botocore.credentials import (
    ContainerProvider,
    CredentialProvider,
    InstanceMetadataProvider,
    RefreshableCredentials,
)
from botocore.exceptions import CredentialRetrievalError

from ...config import SqsConnectionConfig

# botocore refreshes 15 minutes before expiry and rejects a refresh that expires within 10,
# so an entry used by another process must outlive both windows (Laravel uses 60 seconds).
_STALE_SECONDS = 16 * 60
_LOCK_WAIT_SECONDS = 5.0
_FIELDS = ("access_key", "secret_key", "token", "expiry_time")


def credentials_cache_key(connection: SqsConnectionConfig, provider: str) -> str:
    """Get the cache key for the connection's shared credentials.

    The inputs match Laravel's ``SqsConnector::credentialsCacheKey``.
    """
    parts = (
        os.environ.get("AWS_CONTAINER_CREDENTIALS_FULL_URI", ""),
        os.environ.get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", ""),
        provider,
        connection.region,
        connection.prefix,
        connection.suffix,
    )
    return "aws:sqs:credentials:" + hashlib.sha256("|".join(parts).encode()).hexdigest()


class CachedCredentialProvider(CredentialProvider):
    """Resolve refreshable credentials through a file shared by every process on the host.

    A refresh reads the shared file first. On a miss, one process takes an exclusive lock,
    fetches from the wrapped provider and writes the file, while the others wait up to five
    seconds and read it. Any cache failure falls back to a direct fetch. The file is mode
    ``0600`` and is ignored unless the current user owns it.
    """

    def __init__(self, provider: ContainerProvider | InstanceMetadataProvider, key: str) -> None:
        """Create a new cached credential provider instance."""
        super().__init__()
        self._method: str = provider.METHOD
        self._provider = provider
        self._path = os.path.join(tempfile.gettempdir(), key)

    def load(self) -> RefreshableCredentials | None:
        """Load the credentials, or ``None`` when the wrapped provider has none."""
        metadata = self._resolve()
        if metadata is None:
            return None
        return RefreshableCredentials.create_from_metadata(
            metadata, refresh_using=self._refresh, method=self._method
        )

    def _refresh(self) -> dict[str, str]:
        """Get refreshed credential metadata for botocore."""
        metadata = self._resolve()
        if metadata is None:
            raise CredentialRetrievalError(provider=self._method, error_msg="No credentials.")
        return metadata

    def _resolve(self) -> dict[str, str] | None:
        """Get fresh metadata from the shared file, else fetch and share it."""
        if (metadata := self._read()) is not None:
            return metadata
        lock = self._lock()
        try:
            if lock is not None and (metadata := self._read()) is not None:
                return metadata
            metadata = self._fetch()
            if metadata is not None:
                self._write(metadata)
            return metadata
        finally:
            if lock is not None:
                os.close(lock)

    def _fetch(self) -> dict[str, str] | None:
        """Fetch metadata directly from the wrapped provider."""
        credentials = self._provider.load()
        if credentials is None:
            return None
        # botocore keeps the expiry of refreshable credentials private.
        expiry = vars(credentials).get("_expiry_time")
        frozen = credentials.get_frozen_credentials()
        if not (isinstance(expiry, datetime) and frozen.access_key and frozen.secret_key):
            raise CredentialRetrievalError(provider=self._method, error_msg="Incomplete.")
        return {
            "access_key": frozen.access_key,
            "secret_key": frozen.secret_key,
            "token": frozen.token or "",
            "expiry_time": expiry.isoformat(),
        }

    def _read(self) -> dict[str, str] | None:
        """Read the shared metadata if this user owns it and it is fresh."""
        try:
            fd = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd) as file:
                if os.fstat(fd).st_uid != os.geteuid():
                    return None
                data = json.load(file)
            metadata: dict[str, str] = {
                name: value for name in _FIELDS if isinstance(value := data[name], str)
            }
            if len(metadata) != len(_FIELDS):
                return None
            remaining = datetime.fromisoformat(metadata["expiry_time"]) - datetime.now(UTC)
        except (OSError, ValueError, KeyError, TypeError):
            return None
        return metadata if remaining.total_seconds() > _STALE_SECONDS else None

    def _lock(self) -> int | None:
        """Take the exclusive refresh lock, or ``None`` if it is unavailable.

        The kernel releases the lock if its holder dies.
        """
        try:
            fd = os.open(self._path + ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        except OSError:
            return None
        deadline = time.monotonic() + _LOCK_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                if time.monotonic() < deadline:
                    time.sleep(0.05)
                    continue
                os.close(fd)
                return None
            return fd

    def _write(self, metadata: dict[str, str]) -> None:
        """Replace the shared metadata atomically, ignoring any failure."""
        directory, name = os.path.split(self._path)
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(dir=directory, prefix=name + ".")
            with os.fdopen(fd, "w") as file:
                json.dump(metadata, file)
            os.replace(temporary, self._path)
        except OSError:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    os.unlink(temporary)
