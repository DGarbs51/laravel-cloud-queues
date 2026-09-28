"""Dispatch through the HTTP route in an isolated producer process, without an ASGI server."""

from __future__ import annotations

import argparse
import json

from fastapi.testclient import TestClient

from tests.conformance.app import app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job")
    parser.add_argument("--kwargs", default='{"label":"demo"}')
    parser.add_argument("--options", default="{}")
    args = parser.parse_args()
    with TestClient(app) as client:
        response = client.post(
            f"/dispatch/{args.job}",
            json={
                "kwargs": json.loads(args.kwargs),
                "options": json.loads(args.options),
            },
        )
        response.raise_for_status()
        print(json.dumps(response.json()))


if __name__ == "__main__":
    main()
