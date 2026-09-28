# Reuse the shared HTTP endpoint fixture (moto locally, LocalStack in CI).
from harness.pytest_plugin import sqs_endpoint

__all__ = ["sqs_endpoint"]
