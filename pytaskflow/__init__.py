from .client import BackgroundJobClient, Client
from .config import configure as _configure, get_storage
from .serialization.base import BaseSerializer
from .serialization.json_serializer import JsonSerializer

_client: BackgroundJobClient | None = None


def configure(storage) -> None:
    _configure(storage)
    global _client
    _client = None


def get_client(serializer: BaseSerializer | None = None) -> BackgroundJobClient:
    if serializer is not None:
        return BackgroundJobClient(get_storage(), serializer)

    global _client
    if _client is None:
        _client = BackgroundJobClient(get_storage(), JsonSerializer())
    return _client


def get_pytaskflow_client(
    serializer: BaseSerializer | None = None,
) -> BackgroundJobClient:
    return get_client(serializer)


__all__ = [
    "BackgroundJobClient",
    "Client",
    "configure",
    "get_client",
    "get_pytaskflow_client",
    "get_storage",
]
