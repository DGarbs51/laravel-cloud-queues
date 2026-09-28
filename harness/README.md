# Local integration harness

Repository-only Python 3.10+ helpers, independent of `laravel_cloud_queues` and excluded from the
published wheel. The emulator models the public protocol in scope §11, not undocumented agent
internals. Moto is local convenience; LocalStack remains the authoritative SQS CI gate (D9).

Run from the repository root (change `3.10` to `3.14` for the other required interpreter):

```sh
uv run --no-project --python 3.10 --with pytest --with httpx --with 'moto[server]>=5' \
  --with boto3 --with redis python -m pytest -p harness.pytest_plugin tests/harness
```

Load fixtures with `-p harness.pytest_plugin` or register `harness.pytest_plugin` in the root
`conftest.py`. The plugin registers `agent`, `sqs`, `redis`, `socket`, and `subprocess` markers.
Dev dependencies: `pytest`, `httpx`, `moto[server]>=5`, `boto3`, and `redis`. Type/lint checks also
use `mypy`, `boto3-stubs[sqs]`, `types-redis`, and `ruff`. No product-package dependency is needed.

## Agent

```python
import httpx
from harness.agent_emulator import AgentEmulator, delay, status

with AgentEmulator.start(poll_wait=0.1, visibility_timeout=30) as emu:
    message_id = emu.enqueue('{"example": true}', delay=0)
    emu.inject("next", status(503, "unhealthy"), times=2)
    emu.inject("next", delay(0.1))
    with httpx.Client(
        transport=httpx.HTTPTransport(uds=emu.socket_path), base_url="http://localhost"
    ) as client:
        response = client.get("/next")
```

`AgentEmulator(...)` can also be used directly as a context manager. The `agent_emulator` fixture
starts one with defaults. `visibility_timeout=None` models indefinite agent visibility renewal.
`message(id)` returns a detached `Message` dataclass including the full status history, even
once processed. `pending()` includes delayed messages; `in_flight()` excludes expired deliveries.
Both return snapshots. Times use the monotonic clock. Each delivery gets a new receipt handle.

`results` contains detached `Result` records with decoded `body`, `raw_body`, `response_code`,
`applied_code`, and `completed`. Disconnected/hanging requests have no response code.
`applied_code` records the normal result-handler decision, including an acknowledgement applied
before a lost response. `wait_for_result(id, timeout=5)` waits for the first completed request for
that message and raises `TimeoutError` if absent. Use `results` to inspect retries or hanging requests.
Processed messages cannot be acknowledged again; unknown/deleted IDs and stale receipts default to
404 (`unknown_message_status` and `stale_receipt_status` are configurable).

Fault queues are independent for `next` and `result`. Inject a name (`disconnect`, `malformed_json`,
`non_object_json`, `missing_message_id`, `empty_message_id`, `non_string_fields`, `hang`), or use
`status(code, body="")` / `delay(seconds)`. `apply_then_disconnect` is result-only. Response faults
leave message state unchanged; `delay` continues normally; `hang` waits until shutdown.
`faults_fired` records `(endpoint, Fault)` in firing order. JSON-shape faults return synthetic bodies.

Manual listener: `python -m harness.agent_emulator --socket /path/in/private/directory/agent.sock`.
SIGINT/SIGTERM cleanly stop it. A fresh manual listener has no queued messages.

## Collector

`with LogCollector() as collector:` starts a persistent NDJSON Unix listener; `log_collector` provides
it as a fixture. `raw_lines` preserves bytes including trailing newlines; EOF fragments are preserved
and flagged in `errors`. `events` contains parsed JSON values; invalid JSON/UTF-8 and non-object events
are flagged. All properties return snapshots. Pure validators return lists of errors, empty on success:
`validate_lifecycle_event`, `validate_failed_job_event`, and `validate_sequence(events, expected_types)`.
The sequence validator uses lifecycle types and `failed_job` for failure records, in exact order.

`wait_for(lambda events: len(events) == 2, timeout=5)` returns an event snapshot or raises `TimeoutError`.
`close_clients()` forces EOF while keeping the listener. `stop()` closes all connections and unbinds;
`start()` rebinds the same path without clearing captures. Setting `refuse=True` stops the listener;
set it to false and call `start()` to recover. Context exit / `close()` also remove the private directory.
Both Unix services refuse pre-existing paths and only unlink their own socket inode.

## SQS, Redis, and processes

`sqs_endpoint` is both a fixture and a context manager from `harness.sqs`. It yields an `SQSEndpoint`
with `url`, `client`, `region`, `access_key`, `secret_key`, and unique `prefix`.
`create_queue(fifo=False)` returns a queue URL and tracks it for deletion at teardown; FIFO queues
use content-based deduplication. All AWS credentials/config are explicit and isolated from ambient
`AWS_*` settings. Worker subprocesses can use the HTTP URL with the same dummy credentials.

- `LARAVEL_CLOUD_QUEUES_TEST_SQS=moto` (default) starts a loopback HTTP server on an OS-assigned port.
- `LARAVEL_CLOUD_QUEUES_TEST_SQS=localstack` uses `LARAVEL_CLOUD_QUEUES_TEST_SQS_ENDPOINT`
  (default `http://localhost:4566`).
- `redis_url` uses `LARAVEL_CLOUD_QUEUES_TEST_REDIS_URL` (default `redis://127.0.0.1:6379/15`).
  Pair it with `redis_prefix` for prefix-scoped teardown. `redis_service()` offers the same lifetime
  as a context manager yielding `(client, prefix)`; `available()` probes availability. Cleanup uses
  SCAN/DEL and never flushes a database. Tests must put every created Redis key under their prefix.
- Missing SQS/Redis services skip fixture setup unless `LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1`,
  which fails setup. Invalid service configuration and test failures are never converted to skips.

`run_process([command, ...], env={"NAME": "value", "REMOVE_ME": None}, cwd=...)` returns a running
`Process`. `wait(timeout=10)` returns `ProcessResult(returncode, stdout, stderr)`; timeouts raise
`subprocess.TimeoutExpired`. `send_signal(signal.SIGTERM)` targets the process. Stdout/stderr are
captured to distinct files (`stdout_path` / `stderr_path`), preventing pipe backpressure. Fixture
teardown kills surviving process groups. Explicit `Process(...)` contexts have the same behavior;
pass `output_dir` to retain artifacts, otherwise their private output directory is removed on close.
