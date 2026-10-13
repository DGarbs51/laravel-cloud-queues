"""Retrieve Cloud failure records and send them through the dispatch pipeline."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from typing import cast
from urllib.parse import urlsplit

import httpx

from .._narrowing import is_list, is_mapping
from ..errors import ConfigurationError
from ..jobs.dispatch import prepare_dispatch, send_prepared
from ..jobs.execution import prepare_execution
from ..jobs.job import DispatchOptions, Job
from ..observability._events import retried_job_event
from ..registry import Registry
from ._encryption import Encrypter

_logger: logging.Logger = logging.getLogger(__name__)
_MAX_RESPONSE_BYTES = 64 * 1_048_576


def _validate_url(url: str) -> None:
    """Require an absolute HTTPS URL without embedded credentials."""
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname
            and parsed.username is None
            and parsed.password is None
        )
        _ = parsed.port
    except ValueError:
        valid = False
    if not valid:
        raise ConfigurationError("Failed-job retry URLs must use HTTPS without URL credentials.")


def _fetch(client: httpx.Client, url: str) -> tuple[bytes, str]:
    """Fetch a bounded encrypted page, retrying transient failures up to ten times."""
    _validate_url(url)
    attempt = 0
    while True:
        try:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > _MAX_RESPONSE_BYTES:
                        raise ValueError("The failed-job response exceeds the 64 MiB limit.")
                    chunks.append(chunk)
                version = cast(str, response.headers.get("Cloud-Payload-Version", ""))
                return b"".join(chunks), version
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status not in (408, 429) and status < 500:
                raise
            if attempt == 9:
                raise
        except httpx.TransportError:
            if attempt == 9:
                raise
        time.sleep(0.5 * 2**attempt)
        attempt += 1


def _records(client: httpx.Client, url: str, encrypter: Encrypter) -> Iterator[object]:
    """Yield legacy or v1 failure records, following validated pagination links."""
    seen: set[str] = set()
    while True:
        if url in seen:
            raise ValueError("The failed-job pagination contains a cycle.")
        seen.add(url)
        encrypted, version = _fetch(client, url)
        payload: object = json.loads(encrypter.decrypt(encrypted))
        if not is_mapping(payload):
            raise ValueError("The failed-job response must contain an object.")
        if not version:
            yield payload
            return
        if version != "1":
            raise ValueError("Unsupported failed-job payload version.")
        data, links = payload.get("data"), payload.get("links")
        if not is_list(data) or not is_mapping(links):
            raise ValueError("Invalid v1 failed-job response.")
        next_url = links.get("next")
        if next_url is not None and not isinstance(next_url, str):
            raise ValueError("Invalid failed-job pagination link.")
        yield from data
        if next_url is None:
            return
        url = next_url


def _retry_record(registry: Registry, record: object) -> None:
    """Decode and dispatch one failure before reporting that it was retried."""
    if not is_mapping(record):
        raise ValueError("A failed-job record must be an object.")
    job_id, queue, payload = (record.get(key) for key in ("id", "queue", "payload"))
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("A failed-job record must have a non-empty id.")
    if not isinstance(queue, str) or not queue or not isinstance(payload, str):
        raise ValueError("A failed-job record must have a queue and string payload.")
    if record.get("connection") != "cloud":
        raise ValueError("Only Cloud failed-job records are supported.")
    execution = prepare_execution(registry, payload)
    job = Job(
        cast(Callable[[], object], execution.job.func),
        registry=registry,
        name=execution.job.name,
        queue=queue,
        policy=execution.envelope.policy,
    )
    prepared = prepare_dispatch(job, execution.args, execution.kwargs, DispatchOptions(queue=queue))
    send_prepared(job, prepared)
    try:
        registry.telemetry.emit(retried_job_event(job_id, queue, timestamp=datetime.now(UTC)))
    except Exception:
        _logger.warning("Could not emit the failed-job retry event.", exc_info=True)


def retry_failed_jobs(
    registry: Registry,
    urls: Sequence[str],
    *,
    cipher: str,
    report_error: Callable[[Exception], None],
) -> int:
    """Retry all supplied URLs and return one if any page or job fails."""
    for url in urls:
        _validate_url(url)
    encrypter = Encrypter(cipher)
    registry.load()
    failed = False
    with httpx.Client(
        timeout=10,
        follow_redirects=False,
        trust_env=False,
        headers={"Cloud-Payload-Version": "1", "Cloud-Encryption-Cipher": encrypter.cipher},
    ) as client:
        for url in urls:
            try:
                for record in _records(client, url, encrypter):
                    try:
                        _retry_record(registry, record)
                    except Exception as exc:
                        failed = True
                        report_error(exc)
            except Exception as exc:
                failed = True
                report_error(exc)
    return int(failed)
