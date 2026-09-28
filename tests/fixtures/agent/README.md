# Agent protocol fixtures

Synthetic public-protocol examples, not captured production traffic. Each JSON file
is a list of named cases; `raw` is literal HTTP response text, while `body` is JSON
encoded by the test. They are shared by unit/integration tests and future conformance.

Pinned sources (paths below are relative to these public checkouts):

- `framework`: laravel/framework v13.33.0,
  `91188a17ceaa3dbace6e8a5f7abd0d042e466359`.
- `symfony-on-cloud`: `50c945170b6cb5690370d15fd725c6f82495e9ba`.

| Fixture cases | Upstream evidence |
| --- | --- |
| `requests.json`: all | `framework/src/Illuminate/Foundation/Cloud/Queue.php:332` (POST fields, omit only null, 10 s and connection-only retries); `framework/src/Illuminate/Foundation/Cloud/CloudJob.php:39` (processed), `:57` (released) |
| `responses.json`: delivery, missing/empty/non-string/null/zero IDs, arrays, non-string fields | `framework/src/Illuminate/Foundation/Cloud/Queue.php:260`; `framework/tests/Foundation/Cloud/QueueTest.php:970` (coercion), `:982` (string `0`) |
| `responses.json`: empty-204 | `framework/src/Illuminate/Foundation/Cloud/Queue.php:307`; `symfony-on-cloud/tests/AgentClientTest.php:42` |
| `responses.json`: queue URL and fallback | `symfony-on-cloud/src/Queue/Messenger/CloudQueueTransport.php:313`; `symfony-on-cloud/src/Queue/QueueEventSubscriber.php:265`; D13.5 deviation (agent telemetry queue) |
| `responses.json`: attempt defaults | `symfony-on-cloud/src/Queue/Messenger/CloudQueueTransport.php:359`; D13.5 labeled deviation **missing-receive-count-is-one** (Laravel reads 0) |
| `faults.json`: malformed/scalar/null/boolean/number JSON | `framework/src/Illuminate/Foundation/Cloud/Queue.php:317`; `symfony-on-cloud/src/Queue/Agent/AgentClient.php:71` |
| `faults.json`: unexpected-success, redirect | `framework/src/Illuminate/Foundation/Cloud/Queue.php:311`; `framework/tests/Foundation/Cloud/QueueTest.php:1165` |
| `faults.json`: poll-client-error, poll-server-error | `framework/src/Illuminate/Foundation/Cloud/Queue.php:297`; D13.3 (3 attempts, immediate then 500 ms) |
| `faults.json`: result-stale-receipt, result-client-error, result-server-error | `framework/src/Illuminate/Foundation/Cloud/Queue.php:335`; `symfony-on-cloud/src/Queue/Agent/AgentClient.php:126` |

The emulator's 404 for an expired or already-applied receipt is a harness choice,
not a claim about the proprietary agent's exact response code. Only its 4xx
classification is part of the client contract. Likewise emulator visibility timing
is test control, not a reproduction of proprietary heartbeat internals.

Additional package safety checks (no upstream parity claim): stream/cap poll bodies
at 2 MiB; reject unexpected compression (requests advertise identity); discard
unused result/error bodies; suppress sensitive exception chains; interrupt idle
reads with socket shutdown; block every second outcome for the same Delivery.
Laravel's `QueueTest.php:1263` permits explicit release-then-fail; this package's
worker contract deliberately chooses exactly one outcome.

`renew` is a no-op: the agent owns heartbeats and `supports_renewal` is false.
`interrupt` permanently stops polling, preserves already-received deliveries, and
does not cancel result reporting. A report may be applied before its response is
lost; retries reuse the same outcome, but at-least-once job execution still requires
idempotent handlers.
