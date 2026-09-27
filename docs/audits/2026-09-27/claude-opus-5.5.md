# Audit: PROJECT_SCOPE.md and AGENT_BUILD_PROMPT.md — Claude Opus 5.5

Auditor: Claude Opus 5.5 (orchestrator). Read-only audit; no scope or prompt files were edited.

Pinned sources inspected locally:

- `laravel/framework` v13.33.0 (`/tmp/lf`)
- `laravel/symfony-on-cloud` 50c945170b6cb5690370d15fd725c6f82495e9ba (`/tmp/soc`)

Files read in full: Laravel `Foundation/Cloud/Queue.php`, `QueueConnector.php`, `CloudJob.php`, `FailedJobProvider.php`, `Events.php`, `AgentAwareLostConnectionDetector.php`, `CloudManager.php`; Symfony `Queue/Agent/AgentClient.php`, `Queue/ManagedQueueConfig.php`, `Queue/QueueEventSubscriber.php`, `Queue/Sqs/SqsClientFactory.php`, `Observability/Events.php`.
Files read in relevant sections: `CloudBootstrapper.php`, `Queue/SqsQueue.php`, `Queue/Jobs/SqsJob.php`, `Queue/Worker.php`, `Queue/WorkerOptions.php`, Symfony `Queue/Messenger/CloudQueueTransport.php`, Symfony `README.md`, Symfony `tests/` listing.

## Scope assumptions confirmed by source

- 65s `GET /next` long poll; 204 means no work.
- `POST /result` with `processed`, or `released` plus a delay.
- Agent use is decided by the injected `agent.enabled` flag, not by probing the socket.
- FIFO URL rule `{prefix}/{base}{suffix}.fifo` and its inverse normalization.
- Fresh delay cap of 900s; retry visibility clamp of 43,200s; sub-second retry delay rounds up (Symfony behavior; Laravel floors — this is a deviation to label).
- Timeout exit code 124; default tries = 1.
- Log socket default `unix:///tmp/cloud-init.sock` with `LARAVEL_CLOUD_LOG_SOCKET` override.

## Findings: PROJECT_SCOPE.md

1. **`/result` 4xx is not fatal.** Only connection failure and 5xx raise `AgentUnreachableException` (Laravel) / `TransportException` (Symfony). Laravel rethrows a 4xx `RequestException` as a job exception; Symfony reports it and continues. The scope treats every reporting failure as fatal.
2. **`GET /next` 200 without a valid `messageId` is "no job".** Both upstreams keep polling. Only a non-array body is fatal. Non-string `receiptHandle` becomes null; non-string `body` becomes `""`.
3. **Retry budgets are specific.** Laravel retries `GET /next` once after 500ms. `POST /result` gets 3 attempts, 100ms apart, on connection errors only, 10s timeout.
4. **Timeout does not report `released` to the agent.** Laravel emits a `released` lifecycle event, fails the job if tries are exhausted or fail-on-timeout is set, then exits 124. The message returns through visibility expiry. The scope implies an explicit release.
5. **Payload limit is 1,048,576 bytes** (`SqsQueue::MAX_SQS_PAYLOAD_SIZE`). The scope never states a number.
6. **Envelope needs top-level `uuid` and `displayName`.** The dashboard reads `displayName` as the job name (`FailedJobProvider`), and Cloud re-queues a failed payload verbatim when retried from the dashboard (Symfony `CloudQueueTransport::encode` comment). The scope names neither key and has no test for verbatim re-queue.
7. **Failed-job events differ between upstreams.** Laravel sends `exception_preview` (1001 chars) and `job_name` with the full payload and exception. Symfony omits both fields and trims payload/exception to 2000/4000 bytes because of a 16 KiB log-line limit. Trimmed payloads break dashboard retry. The scope must pick a contract.
8. **The agent only serves the worker's assigned queue.** Laravel reads other queues directly from SQS even in agent mode. Each agent message's `queueUrl` decides where the outcome goes and which queue name is normalized.
9. **`LARAVEL_CLOUD_AGENT_SOCKET`** is a Symfony fallback when the JSON omits `agent.socket`. The scope omits it.
10. **`queues` may be a list or a map.** The default queue comes from `connection.queue`, then `queue`.
11. **Queue-not-found comes from the AWS error `AWS.SimpleQueueService.NonExistentQueue`**, translated by SQS client middleware. There is no up-front check against the `queues` list.
12. **Laravel already implements overflow** (`connection.overflow`, `CLOUD_QUEUE_OVERFLOW_*`), storing oversized bodies in a cache and sending an `@pointer`. The config parser must accept these keys.
13. **Laravel worker settings the scope doesn't address:** 128MB memory limit with exit 12, `tries=0` meaning unlimited, `retryUntil`, `maxExceptions`, and a `released` event emitted when the process crashes fatally. Each should be supported or listed as out of scope.
14. **Direct SQS mode has no heartbeat.** A job running longer than the queue's visibility timeout is delivered twice. The scope does not cover this.
15. **Symfony ships an agent emulator** (`tests/Fixtures/agent-server.php`) exercised by `AgentSocketIntegrationTest.php` and `CloudRetryIntegrationTest.php`. Port its behavior and test cases rather than inventing them. (Codex notes this fixture is only a canned stub; the Python emulator must model state beyond it.)
16. **Log socket encoding** is identical in both upstreams: 2s connect/write timeout, persistent connection, reconnect on EOF, JSON with unescaped slashes/unicode, preserved zero fractions, and invalid UTF-8 substituted.
17. **Security:** decoding must never import a module or instantiate a class based on names in the payload. Job names resolve only through the registry; type tags only through registered codecs.

## Findings: AGENT_BUILD_PROMPT.md

1. **No coordination mechanism for the three agents.** Roles are assigned but not the method: orchestration tool, worktrees or branches, who writes shared interfaces first, who settles disagreements, commit cadence.
2. **Optional extras for OpenTelemetry and Pydantic are not named.** Only `[fastapi]` is.
3. The prompt repeats the scope's incorrect blanket rules for items 1, 2 and 4 above, and forbids changing conformance expectations, so an initial wrong test would become permanent.

## Errata (from `verification-by-astra.md`)

The findings above are kept as originally written. Astra's adversarial check found these errors:

- **Finding 1:** Symfony also wraps any other `GuzzleException` as a fatal `TransportException`, so "only connection failure and 5xx" is not exhaustive. That Symfony "continues" after a 4xx comes from a source comment and is not proven by a test.
- **Finding 2:** "Keep polling" should read "returns an empty poll; the worker then follows its lifecycle options". "Only a non-array body is fatal" applies only among the 200-body checks. Connection errors and non-200/204 statuses are also fatal. A JSON array such as `[]` passes `is_array`.
- **Finding 3:** Laravel's `retry([0, 500])` means **3 total attempts**, retrying after 0 ms and 500 ms, not one retry after 500 ms.
- **Finding 4:**
  - Failure checks run first, then the kill. The lifecycle event is `failed` if the job was failed, otherwise `released`.
  - `retryUntil` and `maxExceptions` are further terminal conditions.
  - Exit 124 can fall back to SIGKILL.
  - Redelivery depends on agent/SQS recovery that is not in these repos.
- **Finding 6:** That Cloud re-queues failed payloads verbatim comes from a Symfony source comment, not verified behavior.
- **Finding 7:** Symfony's 2,000-byte cap applies to the inner `body`, not the whole payload. The exception is 4,000 bytes **plus** a truncation marker. The 16 KiB collector limit is a documented rationale, not verified here.
- **Finding 8:**
  - `/result` carries no `queueUrl`, so the delivered `queueUrl` does not route the outcome.
  - Laravel normalizes the queue argument passed to `pop()` for telemetry; only Symfony uses the delivered `queueUrl`.
  - "Agent serves only its assigned queue" comes from Symfony comments.
- **Finding 10:** "`connection.queue`, then `queue`" is Symfony's rule. Laravel's sender uses `connection.queue`; its worker uses `--queue`, then `queue`, then `default`.
- **Finding 14:** a long job **may** be redelivered and run concurrently. Direct receive does not guarantee two deliveries.
- **Finding 15:** only `AgentSocketIntegrationTest` uses `agent-server.php`. `CloudRetryIntegrationTest` uses mocked SQS.
- **Finding 17:** this is a Python security policy recommendation, not an upstream rule.
- **Prompt finding 3:** the build prompt inherits these rules from the scope rather than repeating each one. It also does not make wrong expectations permanent: it forbids changing expectations only "just to make the Python implementation pass", and directs source comparison when unclear.
