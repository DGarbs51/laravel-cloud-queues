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
