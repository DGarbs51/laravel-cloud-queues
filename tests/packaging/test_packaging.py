"""Packaging verification.

Builds the wheel and sdist once per test session, inspects their contents, then
installs each into a fresh virtualenv *outside* this repository checkout (never
editable) and exercises the installed package in a subprocess whose cwd is also
outside the checkout, so nothing can accidentally import from the source tree.
"""

from __future__ import annotations

import ast
import shutil
import subprocess
import sys
import tarfile
import textwrap
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.packaging

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_TAG = f"{sys.version_info.major}.{sys.version_info.minor}"

# These repository-only directories must never reach the built artifacts.
FORBIDDEN_DIR_PREFIXES = (
    "demo/",
    "docs/",
    "tests/",
    "harness/",
    ".github/",
)

# Module names a shipped file must never import (repo-only tooling).
FORBIDDEN_IMPORT_ROOTS = frozenset({"demo", "harness"})

# hatchling unconditionally adds .gitignore to every sdist and it cannot be excluded
# (see https://github.com/pypa/hatch/issues/1203) — harmless, allow it explicitly.
ALLOWED_SDIST_ROOT_FILES = frozenset(
    {"PKG-INFO", "README.md", "LICENSE", "pyproject.toml", ".gitignore"}
)


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=True)


@pytest.fixture(scope="module")
def built_artifacts(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    out_dir = tmp_path_factory.mktemp("dist")
    if shutil.which("uv"):
        _run(["uv", "build", "--out-dir", str(out_dir)], cwd=REPO_ROOT)
    else:
        _run([sys.executable, "-m", "build", "--outdir", str(out_dir)], cwd=REPO_ROOT)

    wheels = sorted(out_dir.glob("*.whl"))
    sdists = sorted(out_dir.glob("*.tar.gz"))
    assert len(wheels) == 1, f"expected exactly one wheel, got {wheels}"
    assert len(sdists) == 1, f"expected exactly one sdist, got {sdists}"
    return {"wheel": wheels[0], "sdist": sdists[0]}


def _venv_python(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _venv_script(venv_dir: Path, name: str) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / f"{name}.exe"
    return venv_dir / "bin" / name


def _make_venv(base_dir: Path, name: str, install_spec: str) -> Path:
    """Create a venv under ``base_dir`` (already outside the repo checkout) and
    install ``install_spec`` (a wheel/sdist path, optionally with an extras suffix
    like ``"<path>[fastapi]"``) into it, non-editable."""
    venv_dir = base_dir / name
    if shutil.which("uv"):
        _run(["uv", "venv", str(venv_dir), "--python", PYTHON_TAG])
        _run(["uv", "pip", "install", "--python", str(_venv_python(venv_dir)), install_spec])
    else:
        _run([sys.executable, "-m", "venv", str(venv_dir)])
        _run([str(_venv_python(venv_dir)), "-m", "pip", "install", install_spec])
    return venv_dir


@pytest.fixture(scope="module")
def wheel_venv(built_artifacts: dict[str, Path], tmp_path_factory: pytest.TempPathFactory) -> Path:
    base = tmp_path_factory.mktemp("wheel-venv")
    return _make_venv(base, "venv", str(built_artifacts["wheel"]))


@pytest.fixture(scope="module")
def sdist_venv(built_artifacts: dict[str, Path], tmp_path_factory: pytest.TempPathFactory) -> Path:
    base = tmp_path_factory.mktemp("sdist-venv")
    return _make_venv(base, "venv", str(built_artifacts["sdist"]))


@pytest.fixture(scope="module")
def fastapi_venv(
    built_artifacts: dict[str, Path], tmp_path_factory: pytest.TempPathFactory
) -> Path:
    base = tmp_path_factory.mktemp("fastapi-venv")
    return _make_venv(base, "venv", f"{built_artifacts['wheel']}[fastapi]")


def _run_in_venv(venv_dir: Path, code: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run ``code`` with the venv's interpreter, cwd outside the repository."""
    assert not cwd.is_relative_to(REPO_ROOT), "test subprocess must not run inside the repo"
    return subprocess.run(
        [str(_venv_python(venv_dir)), "-c", code],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


# --- Wheel/sdist content inspection -----------------------------------------------


def test_wheel_contains_only_package_and_metadata(built_artifacts: dict[str, Path]) -> None:
    with zipfile.ZipFile(built_artifacts["wheel"]) as wheel:
        names = wheel.namelist()

    assert names, "wheel is empty"
    for name in names:
        allowed = name.startswith("laravel_cloud_queues/") or ".dist-info/" in name
        assert allowed, f"unexpected wheel member: {name}"
    for forbidden in FORBIDDEN_DIR_PREFIXES:
        assert not any(forbidden in name for name in names), f"{forbidden} leaked into the wheel"


def test_wheel_has_py_typed(built_artifacts: dict[str, Path]) -> None:
    with zipfile.ZipFile(built_artifacts["wheel"]) as wheel:
        assert "laravel_cloud_queues/py.typed" in wheel.namelist()


def test_sdist_contains_only_package_and_metadata(built_artifacts: dict[str, Path]) -> None:
    with tarfile.open(built_artifacts["sdist"]) as sdist:
        names = sdist.getnames()

    assert names, "sdist is empty"
    stripped = []
    for name in names:
        # every member is prefixed with "<dist-name>-<version>/"
        _, _, rest = name.partition("/")
        stripped.append(rest)

    for rest in stripped:
        if rest == "":
            continue  # the bare top-level directory entry
        allowed = rest in ALLOWED_SDIST_ROOT_FILES or rest.startswith("src/laravel_cloud_queues/")
        assert allowed, f"unexpected sdist member: {rest}"
    for forbidden in FORBIDDEN_DIR_PREFIXES:
        assert not any(forbidden in rest for rest in stripped), f"{forbidden} leaked into the sdist"


def test_sdist_has_py_typed(built_artifacts: dict[str, Path]) -> None:
    with tarfile.open(built_artifacts["sdist"]) as sdist:
        names = sdist.getnames()
    assert any(name.endswith("src/laravel_cloud_queues/py.typed") for name in names)


def _iter_wheel_python_sources(wheel_path: Path) -> list[tuple[str, str]]:
    with zipfile.ZipFile(wheel_path) as wheel:
        return [
            (name, wheel.read(name).decode("utf-8"))
            for name in wheel.namelist()
            if name.endswith(".py")
        ]


def _imported_roots(source: str) -> set[str]:
    tree = ast.parse(source)
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
    return roots


def test_no_shipped_module_imports_repo_only_tooling(built_artifacts: dict[str, Path]) -> None:
    for name, source in _iter_wheel_python_sources(built_artifacts["wheel"]):
        imported = _imported_roots(source)
        offending = imported & FORBIDDEN_IMPORT_ROOTS
        assert not offending, f"{name} imports repo-only module(s): {offending}"


# --- Installed-package behavior (subprocess, cwd outside the repo) ----------------


_CORE_IMPORT_CHECK = textwrap.dedent(
    """
    import sys
    import laravel_cloud_queues as lcq

    forbidden_loaded = set(sys.modules) & {"fastapi", "redis", "opentelemetry"}
    assert not forbidden_loaded, f"unexpectedly imported: {forbidden_loaded}"

    missing = [name for name in lcq.__all__ if not hasattr(lcq, name)]
    assert not missing, f"__all__ names do not resolve: {missing}"

    assert "site-packages" in lcq.__file__, lcq.__file__
    print("OK")
    """
)


def test_wheel_install_core_import_without_extras(wheel_venv: Path, tmp_path: Path) -> None:
    result = _run_in_venv(wheel_venv, _CORE_IMPORT_CHECK, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_sdist_install_core_import_without_extras(sdist_venv: Path, tmp_path: Path) -> None:
    result = _run_in_venv(sdist_venv, _CORE_IMPORT_CHECK, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_fastapi_extra_installs_and_imports(fastapi_venv: Path, tmp_path: Path) -> None:
    result = _run_in_venv(
        fastapi_venv, "import laravel_cloud_queues.fastapi\nprint('OK')\n", cwd=tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


def test_console_entry_point_runs(wheel_venv: Path, tmp_path: Path) -> None:
    script = _venv_script(wheel_venv, "laravel-cloud-queues")
    result = subprocess.run(
        [str(script), "--help"], cwd=tmp_path, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    for command in ("work", "inspect", "conformance"):
        assert command in result.stdout
    # Installed without a checkout: conformance explains how to run it and exits 2 (cli.md).
    result = subprocess.run(
        [str(script), "conformance"], cwd=tmp_path, capture_output=True, text=True, check=False
    )
    assert result.returncode == 2
    assert "repository checkout" in result.stderr


def test_wheel_venv_does_not_see_repo_source(wheel_venv: Path, tmp_path: Path) -> None:
    result = _run_in_venv(
        wheel_venv,
        "import laravel_cloud_queues as lcq\nprint(lcq.__file__)\n",
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    installed_path = Path(result.stdout.strip())
    assert not installed_path.is_relative_to(REPO_ROOT), installed_path
