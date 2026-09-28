"""``python -m tests.integration.worker.apps.short_lease LEASE work ...``: the real CLI with a
short visibility/reservation lease, so redelivery after a timeout or lost lease takes
seconds instead of the 60 s default (the CLI does not expose the lease)."""

from __future__ import annotations

import functools
import sys

from laravel_cloud_queues import cli

if __name__ == "__main__":
    lease = int(sys.argv[1])
    cli.WorkerOptions = functools.partial(cli.WorkerOptions, lease_seconds=lease)  # type: ignore[misc]
    sys.exit(cli.main(sys.argv[2:]))
