#!/usr/bin/env bash
# Run a command inside the public image that mirrors Laravel Cloud's Python runtime:
# python:<version>-slim-bookworm (Debian 12, CPython under /usr/local). Run it on an
# arm64 runner to match Cloud's aarch64 hosts. Cloud's own customer base images live in
# a private registry and add nginx and PGDG apt layers that these tests do not need.
#
# Usage: CLOUD_PYTHON_IMAGE=python:3.12-slim-bookworm .github/scripts/cloud-python.sh uv run pytest ...
#
# Host networking keeps the service containers reachable on localhost. The workspace and
# RUNNER_TEMP are mounted at their host paths so paths in env vars (TLS CA file, reports)
# match inside the container. The host's uv binary (from setup-uv) is mounted in, and uv
# is forced onto the image's interpreter rather than a uv-managed Python.
set -euo pipefail

image="${CLOUD_PYTHON_IMAGE:?CLOUD_PYTHON_IMAGE is required}"
env_args=()
while IFS= read -r name; do
  env_args+=(-e "$name")
done < <(compgen -e | grep -E '^(LARAVEL_CLOUD_QUEUES_|GITHUB_SHA$|UV_LOCKED$)' || true)

mkdir -p "$RUNNER_TEMP/cloud-home"
exec docker run --rm --network host \
  --user "$(id -u):$(id -g)" \
  -v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE" -w "$GITHUB_WORKSPACE" \
  -v "$RUNNER_TEMP:$RUNNER_TEMP" \
  -v "$(command -v uv):/usr/local/bin/uv:ro" \
  -e HOME="$RUNNER_TEMP/cloud-home" \
  -e UV_CACHE_DIR="$RUNNER_TEMP/uv-cache" \
  -e UV_PROJECT_ENVIRONMENT="$RUNNER_TEMP/cloud-venv" \
  -e UV_PYTHON=/usr/local/bin/python3 \
  -e UV_PYTHON_DOWNLOADS=never \
  "${env_args[@]}" \
  "$image" "$@"
