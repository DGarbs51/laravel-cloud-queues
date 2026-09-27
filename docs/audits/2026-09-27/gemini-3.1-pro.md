# Laravel Cloud Queues - Gemini Audit Report

## 1. CRITICAL Severity

### Sync Handler Event Loop Deadlock (Python Runtime)
*   **Issue:** `PROJECT_SCOPE.md` (Sec 13) mandates "sync handlers execute directly in the worker process" and forbids a thread pool ("do not use a thread pool for sync handler execution merely for throughput"). In an AnyIO/asyncio worker, executing a blocking sync handler directly on the main thread will deadlock the event loop. The worker will be unable to handle signals, process `asyncio` timeouts, or perform any background tasks. Furthermore, strictly enforcing timeouts on synchronous code via `signal.alarm()` only works on the main thread on Unix, severely breaking Windows support.
*   **Source:** `PROJECT_SCOPE.md` Sec 13 & 14.
*   **Recommended Fix:** Update the scope to explicitly specify the execution model for sync handlers. Either mandate `concurrent.futures.ProcessPoolExecutor` to allow strict `SIGKILL`-style timeouts, or permit `anyio.to_thread` (thread pool) while acknowledging that strict timeouts on threads require cooperative cancellation or `ctypes` hacks.

### SQS Credential Cache Omission (Config / Security)
*   **Issue:** `PROJECT_SCOPE.md` (Sec 6) completely misses the `credential_cache` configuration. Laravel `CloudBootstrapper.php` injects a `credential_cache` array to cache AWS credentials, avoiding severe rate limiting from the ECS Pod Identity Agent. Without this, the Python worker may bombard the metadata endpoint.
*   **Source:** `laravel/framework` (`src/Illuminate/Foundation/CloudBootstrapper.php`:1558-1577).
*   **Recommended Fix:** Add `credential_cache: {"enabled": bool, "cache": "string", "store": "string"}` to the expected configuration contract and ensure the Python AWS client (boto3) is configured to cache credentials properly.

### Failed-Job Schema Contradiction (Observability)
*   **Issue:** `PROJECT_SCOPE.md` (Sec 15) lists `job name/display name`, `exception preview`, and `exception detail`. However, Laravel and Symfony heavily disagree on this schema due to platform constraints. Laravel emits top-level `exception_preview`, `job_name`, and `exception`. Symfony (`soc`) omits `job_name` and `exception_preview` entirely, instead packing `displayName` into a truncated `payload` JSON string and truncating `exception` to 4000 bytes. Symfony explicitly notes this is a workaround for Fluent Bit / containerd CRI 16KB line limits.
*   **Source (Laravel):** `src/Illuminate/Foundation/Cloud/FailedJobProvider.php`:77-90.
*   **Source (Symfony):** `src/Queue/QueueEventSubscriber.php`:144-159 (and class docblocks regarding CRI limits).
*   **Recommended Fix:** The agents must explicitly choose a path. Recommend adopting Symfony's strict truncation logic to ensure events survive the Fluent Bit pipeline, but emitting Laravel's exact keys (`exception_preview` and `job_name`) for forward compatibility when the CRI issue is resolved.

## 2. HIGH Severity

### Agent Protocol Retry Semantics (Agent Protocol)
*   **Issue:** `PROJECT_SCOPE.md` (Sec 11) specifies the `/next` and `/result` endpoints but misses the exact timeout/retry semantics implemented by the baseline. Laravel uses a 65s timeout with a `[0, 500]` ms backoff on `/next` failures, and a 10s timeout with `retry(3, 100)` (3 attempts, 100ms backoff) for `/result`.
*   **Source (Laravel):** `src/Illuminate/Foundation/Cloud/Queue.php`:297-299 (`/next`) and 335-338 (`/result`).
*   **Recommended Fix:** Explicitly document the `[0, 500]` retry array for `/next` and `3 tries, 100ms backoff` for `/result` so Codex and Grok align on the transient failure thresholds.

### Multi-Agent Coordination Gap on Agent Emulator (`AGENT_BUILD_PROMPT.md`)
*   **Issue:** `AGENT_BUILD_PROMPT.md` instructs Grok to build the conformance emulator and Codex to build the transport, but fails to specify the exact HTTP status codes that define agent behavior. Without defining that 204 means empty, 5xx triggers retries, and 4xx triggers terminal transport exceptions, Grok and Codex will drift.
*   **Source:** `AGENT_BUILD_PROMPT.md` section "Local infrastructure and conformance".
*   **Recommended Fix:** Add a clear protocol table of HTTP status codes (200, 204, 4xx, 5xx) to the prompt and define which agent retry loops they trigger.

### Observability Socket Reconnection (Observability events)
*   **Issue:** `PROJECT_SCOPE.md` (Sec 15) misses that the stream socket must be `STREAM_CLIENT_PERSISTENT` and requires EOF checks and reconnection logic before every write.
*   **Source:** Laravel `src/Illuminate/Foundation/Cloud/Events.php`:1229-1262. Symfony `src/Observability/Events.php`:1279-1287.
*   **Recommended Fix:** Specify that the log socket uses a persistent connection and requires pre-write EOF checks and automatic reconnection to handle agent sidecar restarts.

## 3. MEDIUM Severity

### SQS Overflow / Large Payload Handling (Config Shape)
*   **Issue:** `PROJECT_SCOPE.md` states large payload offloading (S3) is out of scope for v1. However, Laravel passes `overflowStorage` config (`enabled: env('CLOUD_QUEUE_OVERFLOW_ENABLED')`). If the Python package blindly rejects unknown config keys, it will crash in Laravel Cloud.
*   **Source:** `src/Illuminate/Foundation/CloudBootstrapper.php`:1552.
*   **Recommended Fix:** Include `overflow` or `overflowStorage` in the expected config schema as a parsed but explicitly ignored/unsupported feature for v1.

### Exit Status 124 for Timeouts (`AGENT_BUILD_PROMPT.md`)
*   **Issue:** `PROJECT_SCOPE.md` correctly notes "baseline Laravel uses exit status 124" for timeouts. `AGENT_BUILD_PROMPT.md` completely omits this requirement, meaning Codex might default to `sys.exit(1)`.
*   **Source:** `PROJECT_SCOPE.md` Sec 14 vs `AGENT_BUILD_PROMPT.md` Timeout behavior.
*   **Recommended Fix:** Add the explicit requirement for exit code `124` to the Timeout behavior section of `AGENT_BUILD_PROMPT.md`.

## 4. LOW Severity

### FIFO Queue Naming Rules (Queue URL rules)
*   **Issue:** `PROJECT_SCOPE.md` correctly states `{prefix}/{base}{suffix}.fifo`. Laravel accomplishes this via `Str::finish($queue, $suffix).'.fifo'`, while Symfony uses substring replacement. Python implementations might naively append `.fifo` multiple times or place the suffix after the extension.
*   **Source:** `src/Illuminate/Queue/SqsQueue.php`:2362.
*   **Recommended Fix:** Emphasize in `AGENT_BUILD_PROMPT.md` that suffixing must strip any existing `.fifo`, append the environment suffix, and re-append `.fifo`.

### Packaging Exclusion Details (CI/Packaging)
*   **Issue:** `PROJECT_SCOPE.md` and `AGENT_BUILD_PROMPT.md` mandate that `demo/` be excluded from the PyPI artifact, but leave the build backend ambiguous. Default `setuptools` often includes top-level directories accidentally.
*   **Source:** `PROJECT_SCOPE.md` Sec 4.
*   **Recommended Fix:** Explicitly recommend a specific build backend (like Hatch or Poetry) in `AGENT_BUILD_PROMPT.md` and dictate the specific configuration (e.g., `tool.hatch.build.targets.wheel`) to achieve this exclusion.