"""Real HTTP SQS test endpoints, with explicit test credentials and owned queues."""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moto.server import ThreadedMotoServer

REGION = "us-east-1"
ACCESS_KEY = "testing"
SECRET_KEY = "testing"


class ServiceUnavailable(RuntimeError):
    """A requested local service could not be reached or started."""


@dataclass
class SQSEndpoint:
    url: str
    client: Any
    prefix: str = field(default_factory=lambda: f"lcq-{uuid.uuid4().hex}-")
    region: str = REGION
    access_key: str = ACCESS_KEY
    secret_key: str = SECRET_KEY
    _queues: list[str] = field(default_factory=list)

    def create_queue(self, *, fifo: bool = False) -> str:
        name = self.prefix + uuid.uuid4().hex[:8] + (".fifo" if fifo else "")
        attributes = {"FifoQueue": "true", "ContentBasedDeduplication": "true"} if fifo else {}
        url = str(self.client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"])
        self._queues.append(url)
        return url

    def cleanup(self) -> None:
        while self._queues:
            self.client.delete_queue(QueueUrl=self._queues[-1])
            self._queues.pop()


@contextmanager
def sqs_endpoint() -> Iterator[SQSEndpoint]:
    mode = os.environ.get("LARAVEL_CLOUD_QUEUES_TEST_SQS", "moto")
    if mode not in {"moto", "localstack"}:
        raise ValueError("LARAVEL_CLOUD_QUEUES_TEST_SQS must be moto or localstack")
    server: ThreadedMotoServer | None = None
    endpoint: SQSEndpoint | None = None
    client: Any = None
    try:
        try:
            import boto3
            import botocore.session
            from botocore.config import Config
            from botocore.exceptions import BotoCoreError, ClientError
        except ImportError as exc:
            raise ServiceUnavailable("SQS helper requires boto3") from exc
        try:
            if mode == "moto":
                try:
                    from moto.server import ThreadedMotoServer
                except ImportError as exc:
                    raise ServiceUnavailable("SQS helper requires moto[server]") from exc
                # Moto waits forever if its listener thread cannot bind (e.g. a sandbox).
                # Probe that permission synchronously before starting its readiness wait.
                with socket.socket() as probe:
                    probe.bind(("127.0.0.1", 0))
                server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
                server.start()
                host, port = server.get_host_and_port()
                url = f"http://{host}:{port}"
            else:
                url = os.environ.get(
                    "LARAVEL_CLOUD_QUEUES_TEST_SQS_ENDPOINT", "http://localhost:4566"
                )
            # Disable config/env providers on this session without mutating process-global env.
            session = botocore.session.Session(
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
            session.set_config_variable("config_file", os.devnull)
            session.set_config_variable("credentials_file", os.devnull)
            client = boto3.session.Session(
                botocore_session=session,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                aws_session_token="testing",
                region_name=REGION,
            ).client(
                "sqs",
                endpoint_url=url,
                config=Config(
                    connect_timeout=1,
                    read_timeout=2,
                    retries={"max_attempts": 0},
                    proxies={},
                ),
            )
            endpoint = SQSEndpoint(url, client)
            client.list_queues(QueueNamePrefix=endpoint.prefix)
        except (OSError, BotoCoreError, ClientError) as exc:
            raise ServiceUnavailable(f"{mode} SQS endpoint is unavailable: {exc}") from exc
        yield endpoint
    finally:
        try:
            if endpoint is not None:
                endpoint.cleanup()
        finally:
            if client is not None:
                client.close()
            if server is not None:
                server.stop()
