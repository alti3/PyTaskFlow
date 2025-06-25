# PyTaskFlow Implementation Plan - Phase 4: Async, Integrations, and Interactivity

## I. Introduction & Goals

**Phase 4** elevates PyTaskFlow from a capable background job system to a first-class citizen in the modern Python ecosystem. The primary goals are to introduce support for `asyncio`, making it highly efficient for modern I/O-bound workloads, and to build seamless integrations with popular web frameworks like FastAPI and Litestar. This phase also focuses on making the dashboard interactive, allowing users to manage jobs directly from the UI, and formalizing the documentation to encourage adoption.

## II. Key Objectives

1.  **Asyncio Worker Support:** Enable the worker to execute `async def` job functions natively within an event loop.
2.  **Dashboard Interactivity:** Add actions to the dashboard, such as retrying and deleting jobs.
3.  **Framework Integrations:** Create plugins for FastAPI and Litestar to simplify configuration and dependency injection.
4.  **Official Documentation:** Set up a documentation site using a tool like Sphinx or MkDocs.
5.  **SQLAlchemy Storage (Stretch Goal):** Begin implementation of a SQL-based storage backend using SQLAlchemy to offer an alternative to Redis.

## III. Detailed Breakdown & Implementation Steps

### 1. Asyncio Worker Support

This is a major architectural enhancement. The worker needs to be ableto detect `async def` functions and run them correctly.

#### 1.1. `JobPerformer` Enhancement (`pytaskflow/execution/performer.py`)
The performer is the core component that needs to handle both sync and async functions.

```python
# pytaskflow/execution/performer.py
import importlib
import asyncio
from typing import Any, Coroutine

from ..common.exceptions import JobLoadError

def _is_async_func(func) -> bool:
    """Checks if a function is a coroutine function."""
    return asyncio.iscoroutinefunction(func)

def perform_job(module_name: str, func_name: str, args: tuple, kwargs: dict) -> Any:
    """Updated to handle both sync and async job functions."""
    try:
        module = importlib.import_module(module_name)
        target_func = getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        raise JobLoadError(f"Could not load job target: {module_name}.{func_name}") from e

    if _is_async_func(target_func):
        # If the worker is running in an async context, we can await it.
        # Otherwise, we need to run it in a new event loop.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # 'get_running_loop' is py3.7+ and raises if no loop is running
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        coro = target_func(*args, **kwargs)
        return loop.run_until_complete(coro)
    else:
        # Synchronous function execution remains the same
        return target_func(*args, **kwargs)
```

#### 1.2. `Worker` Concurrency Strategy (`pytaskflow/server/worker.py`)
The worker needs a new strategy to manage a pool of `asyncio` tasks instead of threads.

```python
# pytaskflow/server/worker.py
from enum import Enum
import asyncio

class ConcurrencyMode(Enum):
    THREADED = "threaded"
    ASYNCIO = "asyncio"

class Worker:
    def __init__(self, ..., concurrency_mode: ConcurrencyMode = ConcurrencyMode.THREADED):
        # ...
        self.concurrency_mode = concurrency_mode

    def run(self):
        if self.concurrency_mode == ConcurrencyMode.THREADED:
            self._run_threaded()
        elif self.concurrency_mode == ConcurrencyMode.ASYNCIO:
            asyncio.run(self._run_async())
        else:
            raise ValueError(f"Unsupported concurrency mode: {self.concurrency_mode}")

    def _run_threaded(self):
        # This is the existing implementation from Phase 3
        # ...
    
    async def _run_async(self):
        """The main loop for an asyncio-based worker."""
        logger.info(
            f"Starting ASYNC worker on server {self.server_id} with {self.worker_count} "
            f"max tasks for queues: {', '.join(self.queues)}"
        )
        
        in_flight_tasks = set()

        while not self._shutdown_requested:
            try:
                # Heartbeat and schedulers can be run as async tasks themselves
                # Or kept sync for simplicity initially
                self._send_heartbeat()
                self._run_schedulers()

                if len(in_flight_tasks) < self.worker_count:
                    job = self.storage.dequeue(self.queues, timeout_seconds=1)
                    if job:
                        logger.info(f"Picked up job {job.id}. Creating async task.")
                        processor = JobProcessor(job, self.storage, self.serializer)
                        # We need an async version of process()
                        task = asyncio.create_task(processor.process_async())
                        in_flight_tasks.add(task)
                        
                        # Add a callback to remove the task from the set upon completion
                        task.add_done_callback(in_flight_tasks.discard)
                
                # Yield control to the event loop
                await asyncio.sleep(0.1)

            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.info("Shutdown requested. Waiting for active tasks to complete...")
                self._shutdown_requested = True
            except Exception as e:
                logger.error(f"Unhandled exception in async worker main loop: {e}", exc_info=True)
                await asyncio.sleep(5)

        if in_flight_tasks:
            await asyncio.gather(*in_flight_tasks, return_exceptions=True)

        self.storage.remove_server(self.server_id)
        logger.info(f"Async worker {self.server_id} has stopped.")
```
**Note:** `JobProcessor` also needs a `process_async` method that can `await` an async `perform_job`. This change cascades through the execution path.

### 2. Dashboard Interactivity

This requires adding `POST` routes to the dashboard app and wiring them up to the `BackgroundJobClient`.

#### 2.1. New Dashboard Routes (`pytaskflow/dashboard/app.py`)
Add routes for retrying and deleting jobs.

```python
# pytaskflow/dashboard/app.py
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER

# ...

def create_dashboard_app(storage: JobStorage) -> FastAPI:
    app = FastAPI()
    client = BackgroundJobClient(storage) # The dashboard needs a client instance

    # ... existing GET routes ...

    @app.post("/jobs/failed/requeue")
    async def requeue_failed_job(request: Request, job_id: str = Form(...)):
        # In a real app, this would handle a list of job_ids
        client.requeue(job_id)
        # Redirect back to the failed jobs page
        return RedirectResponse(url=app.url_path_for("get_jobs_by_state", state_name="Failed"), status_code=HTTP_303_SEE_OTHER)

    @app.post("/jobs/failed/delete")
    async def delete_failed_job(request: Request, job_id: str = Form(...)):
        client.delete(job_id)
        return RedirectResponse(url=app.url_path_for("get_jobs_by_state", state_name="Failed"), status_code=HTTP_303_SEE_OTHER)
        
    return app
```

#### 2.2. Update Dashboard Template (`jobs.html`)
Add forms and buttons to trigger the new `POST` routes.

```html
<!-- in jobs.html, within the table -->
{% if state_name == 'Failed' %}
<td>
    <div class="btn-group">
        <form action="/jobs/failed/requeue" method="post" style="display:inline;">
            <input type="hidden" name="job_id" value="{{ job.id }}">
            <button type="submit" class="btn btn-sm btn-primary">Retry</button>
        </form>
        <form action="/jobs/failed/delete" method="post" style="display:inline;">
            <input type="hidden" name="job_id" value="{{ job.id }}">
            <button type="submit" class="btn btn-sm btn-danger">Delete</button>
        </form>
    </div>
</td>
{% endif %}
```
**Note:** For bulk actions, JavaScript would be needed to collect checked job IDs and submit them.

#### 2.3. Client and Storage Methods
The `requeue` and `delete` methods on the client need to be backed by the storage implementation.

```python
# pytaskflow/client.py
from .common.states import EnqueuedState, DeletedState

class BackgroundJobClient:
    # ...
    def requeue(self, job_id: str) -> bool:
        """Moves a job back to the Enqueued state, typically from Failed."""
        job = self.storage.get_job_data(job_id)
        if not job:
            return False
            
        return self.storage.set_job_state(
            job_id,
            EnqueuedState(queue=job.queue),
            expected_old_state=FailedState.NAME
        )
        
    def delete(self, job_id: str) -> bool: ... # Already defined in Phase 3
```

### 3. Framework Integrations (`integrations/`)

This involves creating small, pluggable modules for web frameworks.

#### 3.1. FastAPI Integration (`integrations/pytaskflow_fastapi/plugin.py`)

```python
# integrations/pytaskflow_fastapi/plugin.py
from fastapi import FastAPI, Request
from typing import Callable

from pytaskflow import BackgroundJobClient, Worker
from pytaskflow.storage.base import JobStorage

class PyTaskFlowPlugin:
    def __init__(self, app: FastAPI, storage: JobStorage):
        self.app = app
        self.storage = storage
        self.client = BackgroundJobClient(storage)
        self.worker: Optional[Worker] = None

        app.state.pytaskflow_client = self.client
        
        app.add_event_handler("startup", self.startup)
        app.add_event_handler("shutdown", self.shutdown)

    def get_client(self) -> BackgroundJobClient:
        return self.client

    def include_dashboard(self, path: str = "/pytaskflow"):
        from pytaskflow.dashboard.app import create_dashboard_app
        dashboard_app = create_dashboard_app(self.storage)
        self.app.mount(path, dashboard_app)
        
    def run_worker_in_background(self, **worker_options):
        # For development convenience. Not recommended for production.
        import threading
        self.worker = Worker(self.storage, self.client.serializer, **worker_options)
        self._worker_thread = threading.Thread(target=self.worker.run, daemon=True)

    async def startup(self):
        if self._worker_thread:
            self._worker_thread.start()
            
    async def shutdown(self):
        if self.worker:
            self.worker._shutdown_requested = True
        if self._worker_thread:
            self._worker_thread.join(timeout=10)

def add_pytaskflow_to_app(app: FastAPI, storage: JobStorage) -> PyTaskFlowPlugin:
    return PyTaskFlowPlugin(app, storage)

# Usage in a user's app:
# app = FastAPI()
# storage = RedisStorage()
# ptf_plugin = add_pytaskflow_to_app(app, storage)
# ptf_plugin.include_dashboard()
# ptf_plugin.run_worker_in_background(worker_count=2)
#
# @app.post("/users")
# def create_user(client: BackgroundJobClient = Depends(ptf_plugin.get_client)):
#     ...
```

#### 3.2. Litestar Integration (`integrations/pytaskflow_litestar/plugin.py`)
The approach is similar, using Litestar's lifecycle hooks and dependency injection system.

```python
# integrations/pytaskflow_litestar/plugin.py
from litestar import Litestar
from litestar.di import Provide

from pytaskflow import BackgroundJobClient
from pytaskflow.storage.base import JobStorage

def get_client_dependency(app_state: dict) -> BackgroundJobClient:
    return app_state["pytaskflow_client"]

def configure_pytaskflow(app: Litestar, storage: JobStorage):
    client = BackgroundJobClient(storage)
    app.state.pytaskflow_client = client

    # To use it:
    # app = Litestar(..., on_startup=[lambda app: configure_pytaskflow(app, RedisStorage())],
    #                dependencies={"client": Provide(get_client_dependency)})
```

### 4. Documentation

*   **Tool Choice:** MkDocs with the `mkdocs-material` theme is an excellent, modern choice. Sphinx is more powerful but has a steeper learning curve.
*   **Structure:**
    *   `index.md`: Introduction, features, quick start.
    *   `guide/installation.md`: How to install the core package and storage backends.
    *   `guide/configuration.md`: How to configure storage and global options.
    *   `guide/creating-jobs.md`: Detailed examples of `enqueue`, `schedule`, and `add_or_update_recurring`.
    *   `guide/processing-jobs.md`: How to run a worker, configure queues, and handle concurrency.
    *   `guide/dashboard.md`: How to set up and use the dashboard.
    *   `integrations/fastapi.md`: Specific guide for the FastAPI plugin.
    *   `integrations/litestar.md`: Specific guide for Litestar.
    *   `api/index.md`: Auto-generated API reference using a tool like `mkdocstrings`.
*   **Deployment:** Use GitHub Pages or Read the Docs for hosting.

### 5. SQLAlchemy Storage (Stretch Goal)

This is a significant undertaking and might spill into Phase 5.

#### 5.1. Database Schema
Define the schema using SQLAlchemy's declarative base or Core tables.

*   `pytaskflow_jobs`: Stores the main job data.
*   `pytaskflow_job_parameters`: Key-value store for parameters (if not using JSONB).
*   `pytaskflow_job_history`: Append-only log of state transitions.
*   `pytaskflow_job_queues`: Models the job queues. A `status` column (`enqueued`, `processing`) and `fetched_at` timestamp are needed for dequeuing.
*   `pytaskflow_servers`: For server heartbeats.
*   `pytaskflow_recurring_jobs`: For recurring job definitions.

#### 5.2. `SqlStorage` Implementation (`pytaskflow/storage/sql_storage.py`)
*   **Engine and Session Management:** Take a SQLAlchemy engine or connection string. Use a `sessionmaker` for transactions.
*   **Atomicity:** The biggest challenge is the `dequeue` operation. This must be an atomic "fetch and lock". In SQL, this is typically done with `SELECT ... FOR UPDATE SKIP LOCKED` (for PostgreSQL/MySQL 8+) or more complex transaction isolation and locking mechanisms for other databases.
*   **Connections:** The implementation must be careful with connection pooling and session lifecycle management, especially in a multi-threaded worker.

## IV. Summary of Phase 4 Deliverables

*   An enhanced `Worker` that can be configured to run in either `threaded` or `asyncio` mode.
*   The `JobPerformer` can now correctly execute both `def` and `async def` job functions.
*   An interactive dashboard where users can retry and delete failed jobs.
*   First-party integration plugins for FastAPI and Litestar, providing dependency injection for the `BackgroundJobClient` and easy dashboard mounting.
*   A dedicated documentation website with a user guide, tutorials, and an initial API reference.
*   (Optional) An initial, possibly incomplete, implementation of `SqlAlchemyStorage` that proves the concept of a relational database backend.

At the end of this phase, PyTaskFlow will be highly usable in a wide variety of Python applications, from traditional scripts to modern async web frameworks, and its documentation will be ready for a wider audience.