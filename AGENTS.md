# Agent guidelines

## Dependencies and versions

Use uv commands instead of editing `pyproject.toml` or `uv.lock` by hand:

- Bump the package version: `uv version --bump patch` (or `minor`, `major`).
- Add or raise a dependency floor: `uv add "package>=X.Y.Z"` (`--group dev` or `--optional redis` for groups and extras).
- Remove a dependency: `uv remove package`.
- Upgrade locked dependencies: `uv lock --upgrade`, or `uv lock --upgrade-package package` for one.
- Install the locked environment: `uv sync`.

Run `uv run --locked scripts/check.py` before opening a PR.
