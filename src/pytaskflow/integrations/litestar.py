"""Litestar integration helpers for PyTaskFlow."""

from __future__ import annotations

try:
    from litestar import Litestar
    from litestar.datastructures import State
    from litestar.di import Provide
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ImportError(
        "Litestar integration requires 'litestar'. Install with `uv add litestar`."
    ) from exc

from pytaskflow.client import BackgroundJobClient
from pytaskflow.storage.base import JobStorage


def get_pytaskflow_client(state: State) -> BackgroundJobClient:
    return state.pytaskflow_client


def pytaskflow_dependency() -> Provide:
    return Provide(get_pytaskflow_client, sync_to_thread=False)


def configure_pytaskflow(app: Litestar, storage: JobStorage) -> BackgroundJobClient:
    client = BackgroundJobClient(storage)
    app.state.pytaskflow_client = client
    return client
