"""The lazily built, isolated SQS clients shared by dispatch and watchdog threads."""

from __future__ import annotations

import os
from threading import Lock
from typing import TYPE_CHECKING

import boto3.session
import botocore.session
from botocore.config import Config
from botocore.credentials import ContainerProvider, CredentialResolver, InstanceMetadataProvider
from botocore.exceptions import BotoCoreError
from botocore.utils import InstanceMetadataFetcher

from ...config import SqsConnectionConfig, StaticCredentials
from ...errors import ConfigurationError, TransportError

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient


def _build_client(connection: SqsConnectionConfig) -> SQSClient:
    """Build an SQS client for the given connection.

    Unless the default credential chain was requested, the client uses a private session
    that ignores ambient profiles, credential files and endpoint settings. Raises a
    ``ConfigurationError`` if the selected provider returns no credentials.
    """
    credentials = connection.credentials
    if credentials == "default":
        session = boto3.session.Session(region_name=connection.region)
    else:
        # A private session avoids ambient profiles, credential files, region and
        # endpoint settings without changing process-global os.environ.
        core = botocore.session.Session(
            session_vars={
                name: (None, None, default, convert)
                for name, (
                    _,
                    _,
                    default,
                    convert,
                ) in botocore.session.Session.SESSION_VARIABLES.items()
            }
        )
        core.set_config_variable("config_file", os.devnull)
        core.set_config_variable("credentials_file", os.devnull)
        if isinstance(credentials, StaticCredentials):
            session = boto3.session.Session(
                botocore_session=core,
                aws_access_key_id=credentials.key,
                aws_secret_access_key=credentials.secret,
                aws_session_token=credentials.token,
                region_name=connection.region,
            )
        else:
            if credentials == "ecs":
                provider: ContainerProvider | InstanceMetadataProvider = ContainerProvider()
            elif credentials == "instance":
                provider = InstanceMetadataProvider(
                    iam_role_fetcher=InstanceMetadataFetcher(timeout=1, num_attempts=2)
                )
            else:
                raise ConfigurationError("Unknown SQS credentials provider.")
            # Both providers return refreshable credentials, including the refresh
            # callback and SDK locking. Never install the ambient credential chain.
            core.register_component("credential_provider", CredentialResolver([provider]))
            session = boto3.session.Session(botocore_session=core, region_name=connection.region)
        if session.get_credentials() is None:
            raise ConfigurationError(
                "The selected SQS credentials provider returned no credentials."
            )
    # boto3-stubs types client() per service, and the overloads for the services whose
    # stubs are not installed resolve to Unknown.
    return session.client(  # pyright: ignore[reportUnknownMemberType]
        "sqs",
        endpoint_url=connection.endpoint_url,
        verify=True,
        config=Config(
            # botocore-stubs omits this option, which botocore has supported since 1.28.
            ignore_configured_endpoint_urls=True,  # type: ignore[call-arg]  # ty: ignore[unknown-argument]
            connect_timeout=5,
            read_timeout=25,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )


class SqsTransport:
    """The client handling shared by the SQS producer and consumer."""

    def __init__(self, connection: SqsConnectionConfig) -> None:
        """Create a new SQS transport instance."""
        self._connection = connection
        self._client: SQSClient | None = None
        self._client_lock = Lock()
        self._closed = False

    def _get_client(self) -> SQSClient:
        """Get the SQS client, building it on first use.

        This is thread-safe. Raises a ``TransportError`` if the transport is closed or the
        client cannot be initialized.
        """
        # Boto3 sessions are not thread-safe; construct once in the caller's thread.
        # The constructed client and its refreshable credentials support threads.
        with self._client_lock:
            if self._closed:
                raise TransportError("SQS transport is closed.")
            if self._client is None:
                try:
                    self._client = _build_client(self._connection)
                except (BotoCoreError, ValueError, OSError):
                    raise TransportError("SQS client initialization failed.") from None
            return self._client

    def close(self) -> None:
        """Close the SQS client and prevent it from being built again."""
        with self._client_lock:
            self._closed = True
            if self._client is not None:
                self._client.close()
                self._client = None
