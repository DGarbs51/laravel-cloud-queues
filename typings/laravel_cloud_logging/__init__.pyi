# laravel-cloud-logging ships without type hints or a py.typed marker. These stubs cover
# only the names this package uses; mypy and pyright read them from typings/.
import logging

def configure(
    level: str | int | None = None, *, exceptions: bool = True, access_logs: bool = False
) -> dict[str, object]: ...

class MonologFormatter(logging.Formatter):
    def __init__(self, channel: str | None = None) -> None: ...

class CloudHandler(logging.Handler):
    def __init__(self, address: str | None = None) -> None: ...
