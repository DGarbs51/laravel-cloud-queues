"""Enter the FastAPI app lifespan once per worker process.

Verified against FastAPI 0.141.1 and Starlette 1.7.0:

* ``app.router.lifespan_context(app)`` is the context manager FastAPI actually runs.
  Starlette no longer merges ``on_startup`` / ``on_shutdown`` itself. FastAPI's default
  lifespan (``_DefaultLifespan``) still runs those handlers when the app was created
  without a ``lifespan=`` callable. A custom lifespan replaces them, matching FastAPI.
* Included routers are already merged into ``app.router.lifespan_context``.
* A lifespan that yields a mapping exposes that mapping as ``request.state`` during HTTP
  requests. Queue jobs have no request, so yielded identifier keys are copied onto
  ``app.state`` for dependencies to read. Assigning ``app.state`` inside the lifespan
  body is visible the same way.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI

_RESERVED_STATE_KEY = "laravel_cloud_queues"


@asynccontextmanager
async def enter_lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Run application startup, yield for jobs, then run shutdown.

    ``contextlib.asynccontextmanager`` throws a body error into the ``yield``. That
    skips the statements after ``yield`` unless the app wrapped them in ``finally``.
    Catch the error, leave the app lifespan normally so its shutdown still runs, then
    re-raise. This matches an ASGI server, which sends lifespan shutdown as its own
    message rather than as an exception.
    """

    async with app.router.lifespan_context(app) as maybe_state:
        publish_lifespan_state(app, maybe_state)
        try:
            yield
        except BaseException as exc:
            pending: BaseException | None = exc
        else:
            pending = None
    if pending is not None:
        raise pending


def publish_lifespan_state(app: FastAPI, maybe_state: object) -> None:
    """Copy yielded lifespan state onto ``app.state`` for queue jobs.

    Keys that are not identifiers are skipped (``app.state`` is attribute access).
    ``laravel_cloud_queues`` is left as the adapter binding.
    """

    if not isinstance(maybe_state, Mapping):
        return
    for key, value in maybe_state.items():
        if isinstance(key, str) and key.isidentifier() and key != _RESERVED_STATE_KEY:
            setattr(app.state, key, value)
