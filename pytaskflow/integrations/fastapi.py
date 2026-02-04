"""FastAPI integration helpers for PyTaskFlow."""

from __future__ import annotations

import threading
from typing import Optional

try:
    from fastapi import FastAPI
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ImportError(
        "FastAPI integration requires 'fastapi'. Install with `uv add fastapi`."
    ) from exc

from pytaskflow.client import BackgroundJobClient
from pytaskflow.server.worker import Worker
from pytaskflow.storage.base import JobStorage


class PyTaskFlowFastAPIPlugin:
    def __init__(self, app: FastAPI, storage: JobStorage):
        self.app = app
        self.storage = storage
        self.client = BackgroundJobClient(storage)
        self.worker: Optional[Worker] = None
        self._worker_thread: Optional[threading.Thread] = None

        app.state.pytaskflow_client = self.client
        app.add_event_handler("startup", self.startup)
        app.add_event_handler("shutdown", self.shutdown)

    def get_client(self) -> BackgroundJobClient:
        return self.client

    def include_dashboard(self, path: str = "/pytaskflow", debug: bool = False) -> None:
        from pytaskflow.dashboard.app import create_dashboard_app

        dashboard_app = create_dashboard_app(self.client, debug=debug)
        self.app.mount(path, dashboard_app)

    def run_worker_in_background(self, **worker_options) -> "PyTaskFlowFastAPIPlugin":
        self.worker = Worker(self.storage, self.client.serializer, **worker_options)
        self._worker_thread = threading.Thread(target=self.worker.run, daemon=True)
        return self

    async def startup(self) -> None:
        if self._worker_thread:
            self._worker_thread.start()

    async def shutdown(self) -> None:
        if self.worker:
            self.worker._shutdown_requested = True
        if self._worker_thread:
            self._worker_thread.join(timeout=10)


def add_pytaskflow_to_fastapi(
    app: FastAPI, storage: JobStorage
) -> PyTaskFlowFastAPIPlugin:
    return PyTaskFlowFastAPIPlugin(app, storage)
