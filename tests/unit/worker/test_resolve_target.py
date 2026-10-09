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


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A project directory as the working directory, with conventional modules unloaded."""
    conventional = ("main", "app", "api", "app.main", "app.api")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in conventional:
        monkeypatch.delitem(sys.modules, name, raising=False)
    yield tmp_path
    for name in conventional:
        sys.modules.pop(name, None)


def test_pyproject_target(project: Path, module: Callable[[str], str]) -> None:
    name = module(TARGETS)
    (project / "pyproject.toml").write_text(f'[tool.laravel-cloud-queues]\ntarget = "{name}:app"\n')
    assert resolve_target() is sys.modules[name].target


@pytest.mark.parametrize(
    ("pyproject", "error"),
    [
        ("[tool.laravel-cloud-queues]\ntarget = 1\n", "must be a string"),
        ("[tool\n", "not valid TOML"),
    ],
)
def test_invalid_pyproject_target(project: Path, pyproject: str, error: str) -> None:
    (project / "pyproject.toml").write_text(pyproject)
    with pytest.raises(ConfigurationError, match=error):
        resolve_target()


@pytest.mark.parametrize(
    ("files", "attribute"),
    [
        ({"main.py": TARGETS}, "app"),
        ({"main.py": "app = None\n", "app.py": TARGETS.replace("app =", "api =")}, "api"),
        ({"app/__init__.py": "", "app/main.py": TARGETS + "registry = target\n"}, "app"),
        ({"api.py": "registry = None\n", "app/__init__.py": "", "app/api.py": TARGETS}, "app"),
    ],
)
def test_conventional_target_is_discovered(
    project: Path, files: dict[str, str], attribute: str
) -> None:
    (project / "pyproject.toml").write_text('[project]\nname = "x"\n')
    for path, source in files.items():
        (project / path).parent.mkdir(exist_ok=True)
        (project / path).write_text(source)
    target = resolve_target()
    assert target.registry == "the-registry"  # type: ignore[comparison-overlap]


def test_no_target_found(project: Path) -> None:
    with pytest.raises(ConfigurationError, match="No worker target found"):
        resolve_target()
