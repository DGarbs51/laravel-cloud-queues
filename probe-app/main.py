"""Laravel Cloud platform probe.

GET /verify returns what this container can see of the Laravel Cloud
queue/observability contract, so assumptions in docs/audits can be checked
against a real deployment. Secret values are never returned.
"""

import hmac
import json
import os
import platform
import re
import socket
import stat
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

app = FastAPI()

AGENT_SOCKET_DEFAULT = "/tmp/cloud-agent.sock"
LOG_SOCKET_DEFAULT = "unix:///tmp/cloud-init.sock"


def redact(value: str) -> str:
    # Keep shape (host, port, length) so we can see what was injected without leaking it.
    value = re.sub(r"(://[^:/@\s]*):[^@/\s]*@", r"\1:***@", value)  # password in URL userinfo
    value = re.sub(r"\d{6,}", lambda m: "#" * len(m.group()), value)  # account IDs
    return value if len(value) <= 200 else value[:200] + "…"


def shape(value: Any) -> Any:
    """Structure of the managed-queues JSON with account numbers masked."""
    if isinstance(value, dict):
        return {k: shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [shape(v) for v in value]
    if isinstance(value, str):
        return redact(value)
    return value


def env_report() -> dict[str, str]:
    # Test-only: returns every variable unmasked, secrets included.
    return dict(sorted(os.environ.items()))


def managed_queues_config() -> dict[str, Any]:
    raw = os.environ.get("LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG")
    if raw is None:
        return {"present": False}
    try:
        return {"present": True, "valid_json": True, "shape": shape(json.loads(raw))}
    except json.JSONDecodeError as e:
        return {"present": True, "valid_json": False, "error": str(e)}


def socket_report(path: str) -> dict[str, Any]:
    p = Path(path)
    info: dict[str, Any] = {"path": path, "exists": p.exists()}
    if not info["exists"]:
        return info
    mode = p.stat().st_mode
    info["is_socket"] = stat.S_ISSOCK(mode)
    info["mode"] = oct(stat.S_IMODE(mode))
    if info["is_socket"]:
        # Connect only; never write, so no fake events reach the dashboard.
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect(path)
            info["connectable"] = True
        except OSError as e:
            info["connectable"] = False
            info["error"] = str(e)
        finally:
            s.close()
    return info


def agent_socket_path() -> str:
    raw = os.environ.get("LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG")
    try:
        configured = json.loads(raw)["agent"]["socket"] if raw else None
    except (json.JSONDecodeError, KeyError, TypeError):
        configured = None
    return configured or os.environ.get("LARAVEL_CLOUD_AGENT_SOCKET") or AGENT_SOCKET_DEFAULT


@app.get("/")
def health() -> dict[str, str]:
    return {"status": "ok"}


def require_token(token: str) -> None:
    expected = os.environ.get("PROBE_TOKEN")
    if not expected:
        raise HTTPException(503, "Set PROBE_TOKEN in the environment to enable this route.")
    if not hmac.compare_digest(token, expected):
        raise HTTPException(403, "Invalid token.")


@app.get("/verify")
def verify(token: str = Query("")) -> dict[str, Any]:
    require_token(token)

    log_socket = os.environ.get("LARAVEL_CLOUD_LOG_SOCKET", LOG_SOCKET_DEFAULT)
    return {
        "runtime": {
            "python": sys.version,
            "platform": platform.platform(),
            "executable": sys.executable,
            "cwd": os.getcwd(),
            "pid": os.getpid(),
        },
        "env": env_report(),
        "managed_queues_config": managed_queues_config(),
        "sockets": {
            "agent": socket_report(agent_socket_path()),
            "log": socket_report(log_socket.removeprefix("unix://")),
            "tmp_sockets": sorted(
                str(p) for p in Path("/tmp").glob("*") if p.is_socket()
            ),
        },
        "aws_container_credentials": {
            "relative_uri_set": "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" in os.environ,
            "full_uri_set": "AWS_CONTAINER_CREDENTIALS_FULL_URI" in os.environ,
            "token_file_set": "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE" in os.environ,
        },
    }


# Queue jobs and routes (real laravel-cloud-queues package). Imported last because it
# imports `app` and `require_token` from this module.
import probe_jobs  # noqa: E402, F401
