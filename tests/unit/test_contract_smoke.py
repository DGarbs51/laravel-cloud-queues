"""The contract imports cleanly and exposes the public surface."""

import laravel_cloud_queues as lcq
from laravel_cloud_queues import errors
from laravel_cloud_queues.errors import ErrorClass


def test_public_exports() -> None:
    for name in lcq.__all__:
        assert hasattr(lcq, name), name


def test_every_error_has_one_classification() -> None:
    classes = [
        getattr(errors, n)
        for n in errors.__all__
        if isinstance(getattr(errors, n), type) and issubclass(getattr(errors, n), Exception)
    ]
    for cls in classes:
        assert isinstance(cls.classification, ErrorClass), cls


def test_fatal_exit_codes() -> None:
    assert errors.AgentUnavailableError.exit_code == 0
    assert errors.LeaseLostError.exit_code == 1
    assert errors.AmbiguousAcknowledgementError.exit_code == 1
