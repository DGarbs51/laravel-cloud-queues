"""The helpers that enter the FastAPI app lifespan once per worker process.

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

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .._narrowing import is_mapping

_RESERVED_STATE_KEY = "laravel_cloud_queues"
"""The ``app.state`` key that holds the adapter binding."""


@asynccontextmanager
async def enter_lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Run the application startup, yield for jobs, then run the shutdown.

    Because ``contextlib.asynccontextmanager`` throws a body error into the ``yield``,
    which would skip an app's shutdown not wrapped in ``finally``, the error is caught,
    the app lifespan is left normally so its shutdown still runs, and the error is then
    re-raised. This matches an ASGI server, which sends lifespan shutdown as its own
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
    """Copy the yielded lifespan state onto ``app.state`` for queue jobs.

    Keys that are not identifiers are skipped, since ``app.state`` uses attribute access,
    and ``laravel_cloud_queues`` is left as the adapter binding.
    """

    if not is_mapping(maybe_state):
        return
    for key, value in maybe_state.items():
        if isinstance(key, str) and key.isidentifier() and key != _RESERVED_STATE_KEY:
            setattr(app.state, key, value)
