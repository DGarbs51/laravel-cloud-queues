"""Registry rules, loading and the default invoker."""

from __future__ import annotations

import inspect
import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Optional

import anyio
import anyio.lowlevel
import pytest

from laravel_cloud_queues import Job, JobContext, Registry, RetryPolicy
from laravel_cloud_queues.config import QueueConfig
from laravel_cloud_queues.errors import ConfigurationError, UnknownJobError
from laravel_cloud_queues.registry import DefaultInvoker, WorkerTarget, merge_injected


def plain(user_id: int) -> int:
    """Docstring."""
    return user_id * 2


def test_construction_is_lazy() -> None:
    """No configuration, codecs or backend are resolved until needed."""
    registry = Registry()
    assert registry.registry is registry
    assert registry.jobs() == {}
    assert isinstance(registry, WorkerTarget)
    assert isinstance(registry.invoker, DefaultInvoker)


def test_bare_decorator_uses_module_qualname() -> None:
    registry = Registry()
    job = registry.job(plain)
    assert isinstance(job, Job)
    assert job.name == f"{__name__}.plain"
    assert job.queue is None
    assert job.policy == RetryPolicy()
    assert job.registry is registry
    assert job.func is plain
    assert registry.get(job.name) is job
    assert job(21) == 42
    assert job.__name__ == "plain"
    assert job.__doc__ == "Docstring."
    assert inspect.signature(job) == inspect.signature(plain)


def test_decorator_with_arguments() -> None:
    registry = Registry()

    @registry.job(name="emails.send", queue="emails", tries=3, backoff=[1, 5], timeout=30)
    async def send(user_id: int) -> str:
        return f"sent {user_id}"

    assert send.name == "emails.send"
    assert send.queue == "emails"
    assert send.policy == RetryPolicy(tries=3, backoff=(1, 5), timeout=30)
    assert registry.jobs() == {"emails.send": send}
    assert anyio.run(send, 7) == "sent 7"


def test_nested_function_default_name() -> None:
    registry = Registry()

    @registry.job
    def inner() -> None: ...

    assert inner.name == f"{__name__}.test_nested_function_default_name.<locals>.inner"


def test_callable_object_needs_explicit_name() -> None:
    class Handler:
        def __call__(self, order_id: int) -> None: ...

    registry = Registry()
    with pytest.raises(ConfigurationError, match="explicit job name"):
        registry.job(Handler())


def test_shorthand_overrides_policy_fields() -> None:
    registry = Registry()
    base = RetryPolicy(tries=5, backoff=10, timeout=20, fail_on_timeout=True)
    job = registry.job(name="x", policy=base, tries=2, fail_on_timeout=False)(plain)
    assert job.policy == RetryPolicy(tries=2, backoff=10, timeout=20, fail_on_timeout=False)


def test_invalid_policy_fails_at_declaration() -> None:
    registry = Registry()
    with pytest.raises(ConfigurationError):
        registry.job(name="x", tries=-1)
    assert registry.jobs() == {}


def test_duplicate_name_is_a_configuration_error() -> None:
    registry = Registry()
    registry.job(name="dup")(plain)
    with pytest.raises(ConfigurationError, match="dup"):
        registry.job(name="dup")(lambda: None)
    with pytest.raises(ConfigurationError):
        registry.job(name="")(plain)


def test_same_name_in_separate_registries_is_fine() -> None:
    Registry().job(name="a")(plain)
    Registry().job(name="a")(plain)


def test_get_unknown_job() -> None:
    registry = Registry()
    with pytest.raises(UnknownJobError) as info:
        registry.get("json.decoder.JSONDecoder")
    assert info.value.job_name == "json.decoder.JSONDecoder"


def test_get_never_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "colorsys", raising=False)
    with pytest.raises(UnknownJobError):
        Registry().get("colorsys.rgb_to_hsv")
    assert "colorsys" not in sys.modules


def test_jobs_view_is_read_only() -> None:
    registry = Registry()
    registry.job(name="a")(plain)
    view = registry.jobs()
    with pytest.raises(TypeError):
        view["b"] = view["a"]  # type: ignore[index]


def test_varargs_handler_rejected() -> None:
    def handler(*args: int) -> None: ...

    with pytest.raises(ConfigurationError):
        Registry().job(handler)


@pytest.fixture
def package_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """``lcq_fixture_app`` package; every module appends its name to ``tracker.imported``."""
    record = "import lcq_fixture_tracker; lcq_fixture_tracker.imported.append(__name__)\n"
    (tmp_path / "lcq_fixture_tracker.py").write_text("imported: list[str] = []\n")
    root = tmp_path / "lcq_fixture_app"
    (root / "jobs" / "nested").mkdir(parents=True)
    for path in [
        root / "__init__.py",
        root / "standalone.py",
        root / "jobs" / "__init__.py",
        root / "jobs" / "billing.py",
        root / "jobs" / "nested" / "__init__.py",
        root / "jobs" / "nested" / "deep.py",
    ]:
        path.write_text(record)
    monkeypatch.syspath_prepend(str(tmp_path))
    import lcq_fixture_tracker

    yield lcq_fixture_tracker.imported
    for name in list(sys.modules):
        if name.startswith("lcq_fixture_"):
            del sys.modules[name]


def test_load_imports_include_and_discover(package_tree: list[str]) -> None:
    registry = Registry(
        include=["lcq_fixture_app.standalone", sys], discover=["lcq_fixture_app.jobs"]
    )
    assert package_tree == []
    registry.load()
    assert sorted(package_tree) == [
        "lcq_fixture_app",
        "lcq_fixture_app.jobs",
        "lcq_fixture_app.jobs.billing",
        "lcq_fixture_app.jobs.nested",
        "lcq_fixture_app.jobs.nested.deep",
        "lcq_fixture_app.standalone",
    ]


def test_load_is_idempotent(package_tree: list[str]) -> None:
    registry = Registry(include=["lcq_fixture_app.standalone"])
    registry.load()
    del sys.modules["lcq_fixture_app.standalone"]
    registry.load()
    assert package_tree.count("lcq_fixture_app.standalone") == 1


def test_load_surfaces_import_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pkg = tmp_path / "lcq_fixture_broken"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "bad.py").write_text("raise ImportError('boom')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    try:
        with pytest.raises(ImportError, match="boom"):
            Registry(discover=["lcq_fixture_broken"]).load()
    finally:
        for name in [n for n in sys.modules if n.startswith("lcq_fixture_broken")]:
            del sys.modules[name]


def test_lifespan_is_a_noop() -> None:
    async def main() -> None:
        async with Registry().lifespan():
            pass

    anyio.run(main)


def test_telemetry_is_noop_outside_managed_mode() -> None:
    registry = Registry(config=QueueConfig(mode="sqs"))
    registry.telemetry.emit({"type": "queued"})  # must not raise or write


# --- default invoker ------------------------------------------------------------------


def _param(annotation: object) -> inspect.Parameter:
    return inspect.Parameter("p", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotation)


@pytest.mark.parametrize(
    ("annotation", "injected"),
    [
        (JobContext, True),
        ("JobContext", False),  # inspect_handler resolves hints before asking
        (int, False),
        (Optional[JobContext], False),  # noqa: UP045 - exact JobContext only
        (inspect.Parameter.empty, False),
    ],
)
def test_is_injected(annotation: object, injected: bool) -> None:
    assert DefaultInvoker().is_injected(_param(annotation)) is injected


def test_postponed_annotations_are_resolved_before_injection() -> None:
    """This module uses ``from __future__ import annotations``: the handler's annotations
    are strings until inspect_handler resolves them."""

    def handler(order_id: int, context: JobContext) -> None: ...

    assert inspect.signature(handler).parameters["context"].annotation == "JobContext"
    job = Registry().job(handler)
    assert job.signature.injected == ("context",)
    assert job.signature.serialized == ("order_id",)


def test_merge_injected_keeps_positional_shape() -> None:
    def handler(a: int, context: JobContext, /, b: int, *, c: int = 3) -> None: ...

    marker = object()
    bound = merge_injected(inspect.signature(handler), [1, 2], {"c": 4}, {"context": marker})
    assert bound.args == (1, marker, 2)
    assert bound.kwargs == {"c": 4}

    bound = merge_injected(inspect.signature(handler), [1], {"b": 2}, {"context": marker})
    assert bound.args == (1, marker, 2)
    assert bound.kwargs == {}


def _context() -> JobContext:
    return JobContext(job_name="j", uuid="u", message_id="m", queue="q", attempt=1, max_tries=1)


def test_invoke_sync_handler_on_calling_thread_with_injection() -> None:
    registry = Registry()
    seen: list[object] = []

    @registry.job(name="sync")
    def handler(x: int, context: JobContext, *, y: str = "d") -> None:
        seen.extend([x, context, y, threading.get_ident()])

    context = _context()
    anyio.run(registry.invoker.invoke, handler, [1], {"y": "z"}, context)
    assert seen == [1, context, "z", threading.get_ident()]


def test_invoke_awaits_async_handler() -> None:
    registry = Registry()
    seen: list[object] = []

    @registry.job(name="async")
    async def handler(context: JobContext, x: int) -> None:
        await anyio.lowlevel.checkpoint()
        seen.extend([context, x])

    context = _context()
    anyio.run(registry.invoker.invoke, handler, [5], {}, context)
    assert seen == [context, 5]


def test_injected_positional_only_parameter_after_omitted_defaults() -> None:
    registry = Registry()
    seen: list[tuple[int, str, int]] = []

    missing = _context()

    @registry.job(name="defaults")
    def handler(value: int = 7, context: JobContext = missing, /, *, label: str = "ok") -> None:
        assert context is not missing
        seen.append((value, label, context.attempt))

    with registry.testing():
        handler.dispatch()
        handler.dispatch(label="other")
        handler.dispatch(9)
    assert seen == [(7, "ok", 1), (7, "other", 1), (9, "ok", 1)]
