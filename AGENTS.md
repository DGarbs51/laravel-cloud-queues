# AGENTS.md

## What this is

`laravel-cloud-queues` is a typed Python library (`py.typed`, Python 3.11 to 3.14) for Laravel Cloud queues. It has three backends (`managed`, `sqs` and `redis`), a FastAPI adapter, and a worker CLI (`laravel-cloud-queues`, alias `lcq`). It is built with hatchling and managed with uv. Read `docs/architecture.md` and `docs/contract/architecture.md` before you change behavior. `docs/decisions.md` records the decisions (D1 to D15) that comments refer to.

## Commands

```sh
uv sync                                    # install the locked dev environment
uv run --locked scripts/check.py           # every CI gate in parallel, then the 100% coverage gate
uv run ruff check --fix . && uv run ruff format .   # autofix, then format (solo.yml "fix")
uv run pytest tests/unit -q                # fast loop; tests that need a service skip when it is absent
uv run pytest -m redis                     # one marker (markers: [tool.pytest.ini_options])
uv run --isolated --python 3.11 pytest tests/unit   # another interpreter in a throwaway env
uv run ty check && uv run mypy && uv run pyright     # the three type checkers
uv run pyright --verifytypes laravel_cloud_queues --ignoreexternal   # public API type completeness (must be 100%)
uv run --no-project scripts/update_pythons.py        # upgrade uv-managed Pythons
```

- Always run tools through `uv run`. Never use `pip`, `uv pip install` or an activated venv, because they bypass `uv.lock`.
- Use `uv run --with pkg ...` for a one-off tool. Use `uvx` only for tools that `uv.lock` does not pin (CI uses `uvx --from actionlint-py actionlint`).
- Use `--locked`, not `--frozen`. `--locked` fails when `uv.lock` is stale. `--frozen` silently uses the stale lock. CI sets `UV_LOCKED=1`.
- Run `ruff check --unsafe-fixes --diff` and read the diff before you apply unsafe fixes. Use `uv run ruff rule CODE` to explain a rule.

## Dependencies and versioning

Use uv commands. Never hand-edit `pyproject.toml` or `uv.lock` for versions or dependencies, because uv keeps the two in sync and CI rejects a stale lock. Hand-edit only `[tool.*]` config, which no command manages.

- Bump the version: `uv version --bump patch` (or `minor`, `major`). Never add `--frozen`, because then `uv.lock` keeps the old version. `__version__` reads the package metadata, so `pyproject.toml` is the only source.
- Add a dependency or raise its floor: `uv add "pkg>=X.Y"`. For other places, use `--optional redis` (an extra), `--dev` (the dev group) or `--group docs`.
- Remove one: `uv remove pkg` (with the same `--optional`/`--group` flag).
- Upgrade: `uv lock --upgrade` for everything, or `uv lock --upgrade-package pkg` for one package. Then run `uv sync`.
- Optional integrations (`fastapi`, `redis`, `opentelemetry`) are extras. Never move one into core `dependencies`.

## Gates

CI (`.github/workflows/ci.yml`) requires all of these. `scripts/check.py` mirrors them locally.

- ruff check and format, on `src tests .github/scripts scripts`. Rules are in `[tool.ruff]`.
- actionlint on the workflows.
- ty on `src` and `tests/harness`. mypy strict and pyright strict on `src/laravel_cloud_queues` only. See `[tool.ty]`, `[tool.mypy]` and `[tool.pyright]`.
- The public API must score 100% on `pyright --verifytypes`. Annotate attributes assigned in `__init__` and public module-level variables (`logger: logging.Logger = ...`), since inferred types can differ between type checkers. Name an import guard's exception `_exc`, not `exc`, because a module-level `except ... as exc` becomes a public symbol.
- pytest on Python 3.11 to 3.14 inside `python:<v>-slim-bookworm` arm64 (Laravel Cloud's runtime), against LocalStack, Valkey, Redis and TLS Valkey.
- 100% line and branch coverage of the shipped package, combined across all versions (`[tool.coverage]`). Some branches run on only one Python version, so check coverage with `scripts/check.py`, not a single run.
- Packaging tests and `twine check --strict` on the built wheel and sdist.
- The conformance catalog gate (`docs/contract/catalog.json`). Locally, `scripts/check.py` runs every feature except `redis.tls`. The full `python -m tests.conformance` gate fails without CI's TLS Valkey.
- The Sphinx docs build with `-W` (warnings fail).

Locally, tests use moto for SQS and Herd's Valkey at `127.0.0.1:6379/15`. LocalStack, real Redis and TLS Valkey run only in CI.

## Type-checker suppressions

Fix the type first, with `cast`, a `TypeIs` helper (`_narrowing.py`) or a `Protocol` for an untyped library. Suppress only when that fails. All three checkers run on `src`, and each one errors on unused suppressions. Each checker reads only its own comment (pyright has `enableTypeIgnoreComments = false`), so add one comment per checker that actually fails, in this order:

```python
value = call()  # type: ignore[mypy-code]  # pyright: ignore[rule]  # ty: ignore[ty-code]
```

- Never write a bare `# type: ignore`, because mypy rejects it (`ignore-without-code`).
- Never add a comment for a checker that does not fail. That checker reports it as unused.
- `tests/harness` is checked by ty only, so use `# ty: ignore[code]` there. The other test directories are not type-checked.
- For ruff, use `# noqa: CODE` only. For DeepSource, use `# skipcq: CODE - reason`.

## Architecture

- Transports are synchronous and never see envelopes or jobs: they handle `str` bodies only. Async code calls them through `anyio.to_thread.run_sync`.
- There is one dispatch pipeline (`jobs/dispatch.py`) and one execution path (`jobs/execution.py`). The worker and eager test mode share that path. Never add a second one.
- Argument decoding uses the handler's type annotations. Payloads never name a Python type, module or callable, because a payload is untrusted input.
- Job lookup goes through the registry only. Never import a module named in a message.
- Only `fastapi/` imports FastAPI. The worker finds its target by duck typing (`WorkerTarget`).
- Import optional dependencies inside the function that needs them. On `ImportError`, raise `ConfigurationError` with the `pip install "laravel-cloud-queues[extra]"` hint (see `transports/redis/__init__.py`). The packaging tests install the wheel without extras.
- Configuration is explicit. Ambient `REDIS_URL`, `AWS_*` or `AWS_ENDPOINT_URL` never selects a backend or credentials.
- Telemetry is best-effort. A telemetry failure is logged and never raised. All logging goes through `laravel-cloud-logging` (D15). `SIGALRM` paths write straight to stdout, so that they never wait on a logging lock.
- Deviations from Laravel are deliberate and labeled in the catalog. A new deviation needs a catalog entry and a row in `docs/deviations.md` (`tests/docs/test_deviations.py` enforces this).

## Tests

- Layout: `tests/unit/<area>/`, `tests/integration/<backend>/`, plus `conformance`, `runtime_proof`, `packaging`, `typing` (downstream samples checked by ty), `docs` and `harness`.
- Get services from the harness fixtures (`sqs_endpoint`, `redis_url`, `redis_prefix`, `agent_emulator`, `log_collector`). They skip when a service is absent, and fail when `LARAVEL_CLOUD_QUEUES_REQUIRE_SERVICES=1`, as in CI.
- Redis tests use a unique key prefix. Never `FLUSHDB`, because the database is shared.
- Never read ambient `AWS_*`. Set explicit dummy credentials.
- Every fenced `python` block in `README.md` and the site docs must compile. A block after `<!-- runnable -->` also runs. Keep examples valid.
- Use `@pytest.mark.parametrize` for input tables. Name tests `test_<behavior>`.

## Code style

- Every module starts with a docstring, then `from __future__ import annotations`.
- Docstrings follow Laravel's docblocks: one imperative summary line ("Create a new job instance.", "Determine if ..."). Do not add `Args:` or `Returns:` sections, because types live in signatures. Add prose only for contracts: raised errors, thread safety, limits, security.
- Prefix private names with `_`, including private modules (`_loader.py`). Public API packages declare `__all__`. The `transports/<backend>` packages do not.
- Write few comments. A comment explains why, never what.

## Git and PR workflow

- Never commit to `main`. Name branches `<type>/<slug>`: `feat/`, `fix/`, `chore/`, `ci/`, `docs/`, `build/`.
- Write Conventional Commit messages and PR titles (`feat(cli): ...`, `build: ...`). A release PR ends its title with the version: `... (0.2.2)`.
- PRs are squash-merged. Merge commits are disabled, and branches are deleted on merge. A PR body is short bullets, then the local `scripts/check.py` result.
- Run `uv run --locked scripts/check.py` before you push.

## Releasing

1. In the release PR, run `uv version --bump patch`, raise floors with `uv add`, and run `uv lock --upgrade`.
2. After the PR merges, run `gh workflow run publish.yml --ref main`. This publishes to TestPyPI.
3. Run `gh release create vX.Y.Z --target main --generate-notes --title vX.Y.Z`. This publishes to PyPI. The workflow fails if the tag is not `v` plus `uv version --short`.

Both uploads use trusted publishing (OIDC). Never add API tokens.
