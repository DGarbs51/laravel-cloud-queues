# Verification references

Where to check behavior before changing compatibility code or conformance expectations. Prefer source over documentation, and documentation over memory.

## Public sources (pinned baseline)

| Source | Pin | Verify here |
|---|---|---|
| [`laravel/framework`](https://github.com/laravel/framework) | `v13.33.0` (`91188a17ceaa3dbace6e8a5f7abd0d042e466359`) | Canonical queue semantics: `src/Illuminate/Foundation/Cloud/*`, `src/Illuminate/Foundation/CloudBootstrapper.php`, `src/Illuminate/Queue/{SqsQueue,Worker,WorkerOptions,Queue}.php`, `src/Illuminate/Queue/Jobs/{Job,SqsJob}.php`, `src/Illuminate/Queue/Connectors/SqsConnector.php` |
| [`laravel/symfony-on-cloud`](https://github.com/laravel/symfony-on-cloud) | `50c945170b6cb5690370d15fd725c6f82495e9ba` | Cross-framework precedent: `src/Queue/*`, `src/Observability/Events.php`, `tests/` |
| [Laravel Cloud docs](https://laravel.com/cloud/docs) | live | Platform limits and runtime support: `queues`, `runtimes`, `workers`, `environments`, `deployments`, `monorepos` |

## Private Laravel repositories (org access required)

These repositories are internal to the Laravel organization. Do **not** copy their code, configuration or internal implementation details into this public repository. Cite them by name only, and record conclusions as project decisions.

| Repository | Verify here |
|---|---|
| `laravel/cloud` | Control-plane validation, such as which frameworks may create managed queues |
| `laravel/cloud-app-operator` | How managed-queue worker pods are built and run: worker command, agent placement, shutdown and restart behavior |
| `laravel/cloud-init` | Container init: process supervision and the observability log socket |
| `laravel/cloud-logging-collector` | Log pipeline limits that affect `failed_job` event size |
| `laravel/activejob-laravel-cloud` | Rails adapter for managed queues: a non-PHP client of the same agent and event contracts |
| `laravel/cloud-docs` / `laravel/cloud-internal-docs` | Documentation sources, including internal notes |
| Managed-queue agent source | Agent protocol, heartbeat and redelivery. Referenced by `cloud-app-operator`; not accessible with our current permissions |

## Live evidence

- `probe-app/`: deployable probe; `GET /verify` reports what a Cloud container exposes.
- `docs/audits/2026-09-27/platform-findings.md`: results from the first deployment.

## Laravel Cloud CLI (`cpx cloud`)

Run every Laravel Cloud operation through the official CLI, invoked as `cpx cloud <command>` from the repository root. The user has already authenticated it. Do not use the Cloud web API directly, and never read the CLI's credential file.

`.cloud/config.json` (committed) pins the defaults, so commands resolve the right organization and application automatically:

| Resource | ID |
|---|---|
| Organization | Laravel GTM (`org-9fbc6edc-ff47-4f3a-ab70-306d49ff4139`) |
| Application | `laravel-cloud-queues` (`app-a2d97eb6-24b3-4e46-87ef-d3edd9d884f8`), root directory `probe-app`, region `us-east-2` |
| Environment | `production` (`env-a2d97eb8-3c7f-4080-994b-f65c751ed899`), URL `https://laravel-cloud-queues-production-oukrer.laravel-demo.cloud` |
| App cluster instance | `inst-a2d97eb8-43a8-48f8-ab4d-7188083579c8` |
| Worker cluster instance | `inst-a2d9a52c-9797-4568-8a76-5b0d1fccaa74` |

Useful commands (add `--json -n` for machine-readable, non-interactive output):

```sh
cpx cloud list --raw                                              # every command
cpx cloud deployment:list env-a2d97eb8-3c7f-4080-994b-f65c751ed899  # deploy status
cpx cloud deploy laravel-cloud-queues production                  # manual redeploy
cpx cloud environment:logs env-a2d97eb8-3c7f-4080-994b-f65c751ed899 # recent logs
cpx cloud environment:get env-a2d97eb8-3c7f-4080-994b-f65c751ed899
cpx cloud instance:list env-a2d97eb8-3c7f-4080-994b-f65c751ed899
cpx cloud background-process:list <instance-id>
cpx cloud background-process:update <process-id> --command="..."
cpx cloud background-process:create <instance-id> --type=custom --command="..." --processes=1
```

Pitfalls found during testing:

- **Every push to `main` deploys `probe-app/`.** Check the result with `deployment:list` and `environment:logs`.
- **Background process changes take effect only on the next deploy.** A process created while a deploy is running is not picked up; run `cpx cloud deploy` afterwards.
- **`environment:logs` returns only the most recent 100 lines.** Query soon after the event you want, and avoid flooding the logs with requests while checking.
- **Sensitive values are masked by default.** Do not pass `--show-sensitive`.
- **`managed-queue:create` requires a `composer.json` in the working directory,** and the Cloud API currently rejects managed queues for FastAPI apps (`platform-findings.md`).
- **Cloudflare in front of Cloud apps rejects Python `urllib`'s default User-Agent (HTTP 403).** Use `curl` or set a User-Agent header when calling the app over HTTP.
- **Commands that create, delete or change infrastructure may need the user's approval** under the session's permission rules. If one is denied, stop and ask the user; do not work around it.
