from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from botocore.credentials import ContainerProvider, InstanceMetadataProvider, RefreshableCredentials

from laravel_cloud_queues.config import SqsConnectionConfig, StaticCredentials
from laravel_cloud_queues.errors import ConfigurationError, TransportError
from laravel_cloud_queues.transports.sqs import SqsProducer, _client


@pytest.fixture
def hostile_aws(monkeypatch, tmp_path):
    config = tmp_path / "config"
    config.write_text(
        "[profile storage]\nregion = storage-region\n"
        "endpoint_url = http://storage.invalid\naws_access_key_id = storage-key\n"
        "aws_secret_access_key = storage-secret\n"
    )
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[storage]\naws_access_key_id = storage-key\naws_secret_access_key = storage-secret\n"
    )
    for name, value in {
        "AWS_PROFILE": "missing-profile",
        "AWS_DEFAULT_PROFILE": "missing-profile",
        "AWS_CONFIG_FILE": str(config),
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials),
        "AWS_ACCESS_KEY_ID": "storage-key",
        "AWS_SECRET_ACCESS_KEY": "storage-secret",
        "AWS_SESSION_TOKEN": "storage-token",
        "AWS_REGION": "storage-region",
        "AWS_DEFAULT_REGION": "storage-region",
        "AWS_ENDPOINT_URL": "http://storage.invalid",
        "AWS_ENDPOINT_URL_SQS": "http://storage-sqs.invalid",
        "AWS_USE_FIPS_ENDPOINT": "true",
        "AWS_USE_DUALSTACK_ENDPOINT": "true",
        "AWS_CA_BUNDLE": "/missing-ca",
    }.items():
        monkeypatch.setenv(name, value)


def test_static_configuration_ignores_ambient_aws(hostile_aws):
    connection = SqsConnectionConfig(
        "https://sqs.us-east-1.amazonaws.com/123",
        "us-east-1",
        StaticCredentials("explicit-key", "explicit-secret"),
    )
    client = _client._build_client(connection)
    try:
        assert client.meta.region_name == "us-east-1"
        assert client.meta.endpoint_url == "https://sqs.us-east-1.amazonaws.com"
        credentials = client._request_signer._credentials.get_frozen_credentials()
        assert (credentials.access_key, credentials.secret_key, credentials.token) == (
            "explicit-key",
            "explicit-secret",
            None,
        )
        assert client._endpoint.http_session._verify is True
        assert client.meta.config.retries == {"mode": "standard", "total_max_attempts": 3}
    finally:
        client.close()


@pytest.mark.parametrize("mode", ["ecs", "instance"])
def test_selected_provider_is_refreshable_and_exclusive(mode, hostile_aws, monkeypatch):
    """Queue/Connectors/SqsConnector.php:101: explicit ecs/instance providers, D13.4."""
    expiry = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    metadata = {
        "access_key": "role-key",
        "secret_key": "role-secret",
        "token": "role-token",
        "expiry_time": expiry,
    }
    refresh = Mock(return_value={**metadata, "access_key": "refreshed-key"})
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=metadata, refresh_using=refresh, method=mode
    )
    provider_type = ContainerProvider if mode == "ecs" else InstanceMetadataProvider
    other_type = InstanceMetadataProvider if mode == "ecs" else ContainerProvider
    load = Mock(return_value=credentials)
    monkeypatch.setattr(provider_type, "load", load)
    monkeypatch.setattr(other_type, "load", Mock(side_effect=AssertionError("wrong provider")))
    connection = SqsConnectionConfig("https://sqs.us-east-1.amazonaws.com/123", "us-east-1", mode)
    client = _client._build_client(connection)
    try:
        assert client.meta.endpoint_url == "https://sqs.us-east-1.amazonaws.com"
        assert client._request_signer._credentials is credentials
        assert credentials.get_frozen_credentials().access_key == "role-key"
        credentials._expiry_time = datetime.now(UTC) - timedelta(seconds=1)
        assert credentials.get_frozen_credentials().access_key == "refreshed-key"
        load.assert_called_once()
        refresh.assert_called_once()
    finally:
        client.close()


def test_no_provider_credentials_never_falls_back(hostile_aws, monkeypatch):
    monkeypatch.setattr(ContainerProvider, "load", lambda self: None)
    with pytest.raises(ConfigurationError, match="no credentials"):
        _client._build_client(SqsConnectionConfig("prefix", "us-east-1", "ecs"))


def test_unknown_provider_is_rejected():
    with pytest.raises(ConfigurationError, match="Unknown SQS credentials provider"):
        _client._build_client(SqsConnectionConfig("prefix", "us-east-1", "imds-v3"))


def test_default_chain_requires_explicit_opt_in_and_ignores_endpoint(monkeypatch):
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "opt-in-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "opt-in-secret")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://storage.invalid")
    monkeypatch.setenv("AWS_ENDPOINT_URL_SQS", "http://storage-sqs.invalid")
    client = _client._build_client(SqsConnectionConfig("prefix", "us-east-1", "default"))
    try:
        assert client._request_signer._credentials.access_key == "opt-in-key"
        assert client.meta.endpoint_url == "https://sqs.us-east-1.amazonaws.com"
    finally:
        client.close()


def test_default_chain_ignores_endpoint_in_the_aws_config_file(monkeypatch, tmp_path):
    config = tmp_path / "config"
    config.write_text("[default]\nendpoint_url = http://storage.invalid\n")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_SQS", raising=False)
    monkeypatch.delenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "opt-in-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "opt-in-secret")
    client = _client._build_client(SqsConnectionConfig("prefix", "us-east-1", "default"))
    try:
        assert client.meta.endpoint_url == "https://sqs.us-east-1.amazonaws.com"
    finally:
        client.close()


def test_initialization_failure_is_sanitized(monkeypatch):
    monkeypatch.setattr(_client, "_build_client", Mock(side_effect=ValueError("secret")))
    producer = SqsProducer(SqsConnectionConfig("prefix", "us-east-1", "ecs"))
    with pytest.raises(TransportError) as raised:
        producer._get_client()
    assert "secret" not in str(raised.value)
    assert raised.value.__suppress_context__
