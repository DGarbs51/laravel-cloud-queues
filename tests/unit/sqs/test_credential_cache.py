import fcntl
import json
import os
import stat
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from botocore.credentials import ContainerProvider, Credentials, RefreshableCredentials
from botocore.exceptions import CredentialRetrievalError

from laravel_cloud_queues.config import SqsConnectionConfig
from laravel_cloud_queues.transports.sqs import _client, _credential_cache
from laravel_cloud_queues.transports.sqs._credential_cache import (
    CachedCredentialProvider,
    credentials_cache_key,
)

KEY = "aws:sqs:credentials:test"


@pytest.fixture(autouse=True)
def temp_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    return tmp_path


def fetched(access_key="fetched-key", expires_in=timedelta(hours=1)):
    return RefreshableCredentials.create_from_metadata(
        {
            "access_key": access_key,
            "secret_key": "secret",
            "token": "token",
            "expiry_time": (datetime.now(UTC) + expires_in).isoformat(),
        },
        refresh_using=Mock(side_effect=AssertionError("wrapped refresh")),
        method="container-role",
    )


def wrapped(load=None):
    return Mock(METHOD="container-role", load=load or Mock(side_effect=lambda: fetched()))


def entry(access_key="cached-key", expires_in=timedelta(hours=1), **changes):
    return {
        "access_key": access_key,
        "secret_key": "secret",
        "token": "token",
        "expiry_time": (datetime.now(UTC) + expires_in).isoformat(),
        **changes,
    }


def write(path, data):
    (path / KEY).write_text(data if isinstance(data, str) else json.dumps(data))


def access_key(provider):
    return provider.load().get_frozen_credentials().access_key


def test_processes_share_one_fetch(temp_dir):
    """Queue/AwsCredentialCache.php resolve: one refresh under the lock, the rest read it."""

    def slow_fetch():
        time.sleep(0.2)
        return fetched()

    provider = wrapped(Mock(side_effect=slow_fetch))
    barrier = threading.Barrier(8)
    keys = []

    def worker():
        cache = CachedCredentialProvider(provider, KEY)
        barrier.wait()
        keys.append(access_key(cache))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert keys == ["fetched-key"] * 8
    provider.load.assert_called_once()
    assert stat.S_IMODE((temp_dir / KEY).stat().st_mode) == 0o600
    assert json.loads((temp_dir / KEY).read_text())["access_key"] == "fetched-key"


def test_fresh_entry_is_used_without_fetching(temp_dir):
    write(temp_dir, entry())
    provider = wrapped()
    assert access_key(CachedCredentialProvider(provider, KEY)) == "cached-key"
    provider.load.assert_not_called()


def test_stale_entry_is_refreshed_and_shared(temp_dir):
    write(temp_dir, entry(expires_in=timedelta(minutes=15)))
    provider = wrapped()
    assert access_key(CachedCredentialProvider(provider, KEY)) == "fetched-key"
    provider.load.assert_called_once()
    assert json.loads((temp_dir / KEY).read_text())["access_key"] == "fetched-key"


def test_botocore_refresh_reads_the_shared_entry(temp_dir):
    provider = wrapped()
    credentials = CachedCredentialProvider(provider, KEY).load()
    write(temp_dir, entry("shared-key"))
    credentials._expiry_time = datetime.now(UTC) - timedelta(seconds=1)
    assert credentials.get_frozen_credentials().access_key == "shared-key"
    provider.load.assert_called_once()


def test_waiter_rereads_the_entry_after_the_lock(temp_dir):
    holder = os.open(temp_dir / f"{KEY}.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)
    provider = wrapped()
    keys = []
    waiter = threading.Thread(
        target=lambda: keys.append(access_key(CachedCredentialProvider(provider, KEY)))
    )
    waiter.start()
    time.sleep(0.2)
    write(temp_dir, entry())
    os.close(holder)
    waiter.join()
    assert keys == ["cached-key"]
    provider.load.assert_not_called()


def test_lock_timeout_fetches_directly(temp_dir, monkeypatch):
    monkeypatch.setattr(_credential_cache, "_LOCK_WAIT_SECONDS", 0.1)
    holder = os.open(temp_dir / f"{KEY}.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        assert access_key(CachedCredentialProvider(wrapped(), KEY)) == "fetched-key"
    finally:
        os.close(holder)
    assert json.loads((temp_dir / KEY).read_text())["access_key"] == "fetched-key"


@pytest.mark.parametrize(
    "data",
    [
        "garbage",
        "[]",
        "null",
        {"access_key": "cached-key"},
        entry(token=None),
        entry(expiry_time="tomorrow"),
        entry(expiry_time="2999-01-01T00:00:00"),
    ],
)
def test_corrupted_entry_fetches_directly(temp_dir, data):
    write(temp_dir, data)
    assert access_key(CachedCredentialProvider(wrapped(), KEY)) == "fetched-key"
    assert json.loads((temp_dir / KEY).read_text())["access_key"] == "fetched-key"


def test_foreign_or_linked_entry_is_ignored(temp_dir, monkeypatch):
    target = temp_dir / "planted"
    target.write_text(json.dumps(entry("planted-key")))
    (temp_dir / KEY).symlink_to(target)
    assert access_key(CachedCredentialProvider(wrapped(), KEY)) == "fetched-key"
    assert json.loads(target.read_text())["access_key"] == "planted-key"
    monkeypatch.setattr(os, "geteuid", lambda: -1)
    assert access_key(CachedCredentialProvider(wrapped(), KEY)) == "fetched-key"


def test_unwritable_cache_fetches_directly(temp_dir, monkeypatch):
    # A missing directory, because CI's root user ignores permission bits.
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir / "missing"))
    provider = wrapped()
    assert access_key(CachedCredentialProvider(provider, KEY)) == "fetched-key"
    assert access_key(CachedCredentialProvider(provider, KEY)) == "fetched-key"
    assert provider.load.call_count == 2
    assert os.listdir(temp_dir) == []


def test_failed_replace_removes_the_temporary_file(temp_dir):
    (temp_dir / KEY).mkdir()
    assert access_key(CachedCredentialProvider(wrapped(), KEY)) == "fetched-key"
    assert sorted(os.listdir(temp_dir)) == [KEY, f"{KEY}.lock"]


def test_missing_credentials_are_not_cached(temp_dir):
    cache = CachedCredentialProvider(wrapped(Mock(return_value=None)), KEY)
    assert cache.load() is None
    with pytest.raises(CredentialRetrievalError):
        cache._refresh()
    assert not (temp_dir / KEY).exists()


def test_credentials_without_expiry_are_rejected(temp_dir):
    cache = CachedCredentialProvider(wrapped(Mock(return_value=Credentials("a", "b"))), KEY)
    with pytest.raises(CredentialRetrievalError):
        cache.load()
    assert not (temp_dir / KEY).exists()


def test_key_matches_laravel(monkeypatch):
    """SqsConnector::credentialsCacheKey; digest computed with PHP's hash('sha256', ...)."""
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://169.254.170.23/v1/credentials")
    monkeypatch.setenv(
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token",
    )
    connection = SqsConnectionConfig(
        "https://sqs.us-east-2.amazonaws.com/123456789012", "us-east-2", "ecs", suffix="-prod"
    )
    assert credentials_cache_key(connection, "ecs") == (
        "aws:sqs:credentials:818849d7354d3bc9c66eb52f9812c49d8ccb8ef30acb6bc5783d818eebca689f"
    )
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_FULL_URI")
    assert credentials_cache_key(connection, "ecs") != credentials_cache_key(connection, "instance")


@pytest.mark.parametrize("enabled", [True, False])
def test_client_uses_the_cache_only_when_enabled(temp_dir, monkeypatch, enabled):
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", raising=False)
    monkeypatch.delenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", raising=False)
    monkeypatch.setattr(ContainerProvider, "load", lambda self: fetched())
    connection = SqsConnectionConfig(
        "https://sqs.us-east-1.amazonaws.com/123", "us-east-1", "ecs", credential_cache=enabled
    )
    client = _client._build_client(connection)
    try:
        assert client._request_signer._credentials.access_key == "fetched-key"
    finally:
        client.close()
    assert (temp_dir / credentials_cache_key(connection, "ecs")).exists() is enabled
