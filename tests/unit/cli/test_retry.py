"""Check retry command arguments, target resolution, and redacted errors."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

import laravel_cloud_queues.cli as cli
from laravel_cloud_queues import Registry


def test_retry_help_lists_options() -> None:
    result = CliRunner().invoke(cli.cli, ["retry", "--help"])
    assert result.exit_code == 0
    assert "URL [URL...]" in result.output
    assert "--target" in result.output and "--cipher" in result.output


def test_retry_requires_url() -> None:
    result = CliRunner().invoke(cli.cli, ["retry"])
    assert result.exit_code == 2
    assert "Missing argument" in result.output


@pytest.mark.parametrize("target_source", ["option", "pyproject", "convention"])
def test_retry_resolves_target_and_accepts_multiple_urls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    target_source: str,
) -> None:
    registry = Registry()
    module = ModuleType("main")
    module.registry = registry
    monkeypatch.setitem(sys.modules, "main", module)
    monkeypatch.chdir(tmp_path)
    args = ["retry", "https://cloud.test/one", "https://cloud.test/two", "--cipher", "aes-128-gcm"]
    if target_source == "option":
        args += ["--target", "main:registry"]
    elif target_source == "pyproject":
        (tmp_path / "pyproject.toml").write_text(
            '[tool.laravel-cloud-queues]\ntarget="main:registry"\n'
        )
    retrier = Mock(return_value=0)
    monkeypatch.setattr(cli, "retry_failed_jobs", retrier)
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 0, result.output
    assert retrier.call_args.args == (
        registry,
        ("https://cloud.test/one", "https://cloud.test/two"),
    )
    assert retrier.call_args.kwargs["cipher"] == "AES-128-GCM"


@pytest.mark.parametrize("debug", [False, True])
def test_retry_reports_partial_failure_with_redacted_errors(
    monkeypatch: pytest.MonkeyPatch,
    debug: bool,
) -> None:
    monkeypatch.setattr(cli, "resolve_target", lambda _: SimpleNamespace(registry=object()))

    def retrier(registry, urls, *, cipher, report_error) -> int:
        try:
            raise ValueError("https://user:pass@cloud.test/job?signature=SIGNEDSECRET")
        except ValueError as exc:
            report_error(exc)
        return 1

    monkeypatch.setattr(cli, "retry_failed_jobs", retrier)
    args = ["retry", "https://cloud.test/job", *(["--debug"] if debug else [])]
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 1
    assert "SIGNEDSECRET" not in result.output and "user:pass" not in result.output
    assert "ValueError" in result.output
    assert ("Traceback" in result.output) == debug


@pytest.mark.parametrize("url", ["https://cloud.test/job", "http://cloud.test/job"])
def test_retry_configuration_errors_exit_two(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setattr(cli, "resolve_target", lambda _: SimpleNamespace(registry=Registry()))
    monkeypatch.delenv("LARAVEL_CLOUD_QUEUES_KEY", raising=False)
    monkeypatch.delenv("APP_KEY", raising=False)
    result = CliRunner().invoke(cli.cli, ["retry", url])
    assert result.exit_code == 2
    assert ("KEY" if url.startswith("https:") else "HTTPS") in result.output
