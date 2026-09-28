from __future__ import annotations

import sys
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from laravel_cloud_queues.errors import ConfigurationError
from laravel_cloud_queues.worker import resolve_target

TARGETS = """
from contextlib import asynccontextmanager
from types import SimpleNamespace


class Target:
    registry = "the-registry"

    @asynccontextmanager
    async def lifespan(self):
        yield


target = Target()
app = SimpleNamespace(state=SimpleNamespace(laravel_cloud_queues=target))
bare_app = SimpleNamespace(state=SimpleNamespace())
nested = SimpleNamespace(inner=target)
"""


@pytest.fixture
def module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[str], str]]:
    monkeypatch.syspath_prepend(str(tmp_path))
    names: list[str] = []

    def write(source: str) -> str:
        name = f"lcq_target_{uuid.uuid4().hex}"
        (tmp_path / f"{name}.py").write_text(source)
        names.append(name)
        return name

    yield write
    for name in names:
        sys.modules.pop(name, None)


def test_object_with_registry_and_lifespan(module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    assert resolve_target(f"{name}:target").registry == "the-registry"  # type: ignore[comparison-overlap]


def test_app_with_bound_integration(module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    target = resolve_target(f"{name}:app")
    assert target is sys.modules[name].target


def test_dotted_attribute(module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    assert resolve_target(f"{name}:nested.inner") is sys.modules[name].target


@pytest.mark.parametrize("spec", ["nocolon", ":attr", "module:"])
def test_malformed_spec(spec: str) -> None:
    with pytest.raises(ConfigurationError, match="module:attribute"):
        resolve_target(spec)


def test_missing_module() -> None:
    with pytest.raises(ConfigurationError, match="Cannot import module"):
        resolve_target("lcq_definitely_missing.sub:app")


def test_missing_attribute(module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    with pytest.raises(ConfigurationError, match="no attribute"):
        resolve_target(f"{name}:missing")


def test_not_a_target(module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    with pytest.raises(ConfigurationError, match="not a worker target"):
        resolve_target(f"{name}:bare_app")


def test_import_errors_inside_the_app_propagate(module: Callable[[str], str]) -> None:
    name = module("import lcq_missing_dependency_xyz\n")
    with pytest.raises(ModuleNotFoundError):
        resolve_target(f"{name}:app")
