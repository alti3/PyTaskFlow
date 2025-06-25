# PyTaskFlow Implementation Plan - Phase 3: Concurrency and Visibility

## I. Introduction & Goals

**Phase 3** focuses on scaling the worker's processing capabilities and providing visibility into the system's operations. We will introduce concurrency to the worker, allowing it to process multiple jobs simultaneously. We will also build the foundational read-only version of the web dashboard, a critical feature for monitoring and diagnostics, inspired directly by Hangfire's highly-regarded UI. This phase also introduces more states to handle the full lifecycle of a job, including manual deletion and continuations.

## II. Key Objectives

1.  **Concurrent Worker:** Enhance the `Worker` to support multi-threaded execution for I/O-bound jobs.
2.  **Dashboard Foundation:** Create a basic, read-only web dashboard using FastAPI and Jinja2.
3.  **Expanded State Machine:** Implement the `DeletedState` and `AwaitingState`.
4.  **Monitoring API:** Flesh out the monitoring API in the `JobStorage` contract to provide the data needed by the dashboard.

## III. Detailed Breakdown & Implementation Steps

### 1. Concurrent Worker (`pytaskflow/server/worker.py`)

The single-threaded worker from Phase 1/2 is a bottleneck. We'll now use a thread pool to process jobs concurrently.

#### 1.1. `Worker` Class Refactoring
The `Worker` class will be updated to manage a `ThreadPoolExecutor`.

```python
# pytaskflow/server/worker.py
import time
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from ..storage.base import JobStorage
from ..serialization.base import BaseSerializer
from .processor import JobProcessor

logger = logging.getLogger(__name__)

class Worker:
    def __init__(
        self,
        storage: JobStorage,
        serializer: BaseSerializer,
        queues: List[str] = ["default"],
        worker_count: int = 4, # New parameter
        scheduler_poll_interval_seconds: int = 15
    ):
        self.storage = storage
        self.serializer = serializer
        self.queues = queues
        self.worker_count = worker_count
        self.scheduler_poll_interval = scheduler_poll_interval_seconds
        
        self.server_id = f"server:{uuid.uuid4()}"
        self._shutdown_requested = False
        self._last_scheduler_run = 0
        self._last_heartbeat = 0
        
    # _run_schedulers, _enqueue_due_scheduled_jobs, etc. remain the same

    def _send_heartbeat(self):
        """Sends a heartbeat to the storage to indicate the server is alive."""
        now = time.monotonic()
        if (now - self._last_heartbeat) < 30: # Heartbeat every 30s
            return
        
        logger.debug(f"Sending heartbeat for server {self.server_id}")
        self.storage.server_heartbeat(self.server_id, self.worker_count, self.queues)
        self._last_heartbeat = now
        
    def run(self):
        """Updated worker loop to manage a thread pool."""
        logger.info(
            f"Starting worker on server {self.server_id} with {self.worker_count} "
            f"max workers for queues: {', '.join(self.queues)}"
        )
        
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            # Keep a set of future objects for jobs currently in flight
            in_flight_futures = set()
            
            while not self._shutdown_requested:
                try:
                    self._send_heartbeat()
                    self._run_schedulers()

                    # Submit new jobs only if we have available worker slots
                    if len(in_flight_futures) < self.worker_count:
                        job = self.storage.dequeue(self.queues, timeout_seconds=1)
                        if job:
                            logger.info(f"Picked up job {job.id}. Submitting to executor.")
                            processor = JobProcessor(job, self.storage, self.serializer)
                            future = executor.submit(processor.process)
                            in_flight_futures.add(future)

                    # Process completed futures to free up slots
                    if not in_flight_futures:
                        # If no jobs are running, sleep a bit to prevent a tight loop
                        time.sleep(0.1)
                        continue

                    # Use a short timeout to remain responsive to shutdown signals
                    done_futures = set(f for f in as_completed(in_flight_futures, timeout=0.1) if f.done())

                    for future in done_futures:
                        in_flight_futures.remove(future)
                        try:
                            # result() will re-raise any exceptions from the job processor
                            future.result()
                        except Exception as e:
                            logger.error(f"A job processor task failed unexpectedly: {e}", exc_info=True)

                except KeyboardInterrupt:
                    logger.info("Shutdown requested. Waiting for active jobs to complete...")
                    self._shutdown_requested = True
                except Exception as e:
                    logger.error(f"Unhandled exception in worker main loop: {e}", exc_info=True)
                    time.sleep(5)
            
            # Graceful shutdown: wait for all submitted jobs to finish
            for future in as_completed(in_flight_futures):
                logger.info(f"Waiting for job {future} to complete...")
                future.result() # Wait for completion and handle any last-minute errors
        
        self.storage.remove_server(self.server_id)
        logger.info(f"Worker {self.server_id} has stopped.")
```

**Implementation Notes:**
*   The `ThreadPoolExecutor` is the natural choice for managing a pool of worker threads.
*   The main loop becomes a manager: it dequeues jobs and submits them to the executor. It no longer processes jobs directly.
*   `as_completed` with a timeout allows the loop to remain responsive and check for shutdown requests while waiting for jobs to finish.
*.  Heartbeats are now crucial for dashboard visibility.

### 2. Dashboard Foundation (`pytaskflow/dashboard/`)

This involves setting up a basic web application to display job information. FastAPI is a great choice due to its speed and modern features, and Jinja2 is a standard for templating.

#### 2.1. Dependencies
Add `fastapi`, `uvicorn`, and `jinja2` to `pyproject.toml`.

#### 2.2. Web Application (`pytaskflow/dashboard/app.py`)

```python
# pytaskflow/dashboard/app.py
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from ..storage.base import JobStorage

def create_dashboard_app(storage: JobStorage) -> FastAPI:
    app = FastAPI()
    
    # In a real package, templates and static would be handled with importlib.resources
    app.mount("/static", StaticFiles(directory="pytaskflow/dashboard/static"), name="static")
    templates = Jinja2Templates(directory="pytaskflow/dashboard/templates")

    @app.get("/")
    async def get_dashboard(request: Request):
        stats = storage.get_statistics()
        # In a real app, you'd fetch more data for the home page
        return templates.TemplateResponse(
            "dashboard.html", 
            {"request": request, "stats": stats}
        )
        
    @app.get("/jobs/{state_name}")
    async def get_jobs_by_state(request: Request, state_name: str, page: int = 1):
        per_page = 20
        start = (page - 1) * per_page
        
        jobs = storage.get_jobs_by_state(state_name, start, per_page)
        total_count = storage.get_statistics().get(state_name, 0)
        
        return templates.TemplateResponse(
            "jobs.html",
            {
                "request": request,
                "state_name": state_name,
                "jobs": jobs,
                "current_page": page,
                "total_count": total_count,
                "per_page": per_page,
            }
        )

    return app
```

#### 2.3. Templates (`pytaskflow/dashboard/templates/`)
Create basic Jinja2 templates.

`layout.html`:
```html
<!DOCTYPE html>
<html>
<head>
    <title>PyTaskFlow Dashboard</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container-fluid">
            <a class="navbar-brand" href="/">PyTaskFlow</a>
        </div>
    </nav>
    <div class="container mt-4">
        {% block content %}{% endblock %}
    </div>
</body>
</html>
```

`dashboard.html`:
```html
{% extends "layout.html" %}
{% block content %}
<h1>Dashboard</h1>
<div class="row">
    <div class="col-md-2">
        <div class="card text-center">
            <div class="card-body">
                <h5 class="card-title">Succeeded</h5>
                <p class="card-text fs-2">{{ stats.succeeded }}</p>
                <a href="/jobs/Succeeded" class="btn btn-sm btn-success">View</a>
            </div>
        </div>
    </div>
    <div class="col-md-2">
        <div class="card text-center">
            <div class="card-body">
                <h5 class="card-title">Failed</h5>
                <p class="card-text fs-2 text-danger">{{ stats.failed }}</p>
                <a href="/jobs/Failed" class="btn btn-sm btn-danger">View</a>
            </div>
        </div>
    </div>
    <!-- ... More stat boxes for Enqueued, Processing, etc. ... -->
</div>
{% endblock %}
```

`jobs.html`:
```html
{% extends "layout.html" %}
{% block content %}
<h1>{{ state_name }} Jobs</h1>
<table class="table">
    <thead>
        <tr>
            <th>ID</th>
            <th>Job</th>
            <th>State</th>
            <th>Created At</th>
        </tr>
    </thead>
    <tbody>
        {% for job in jobs %}
        <tr>
            <td>{{ job.id[:8] }}...</td>
            <td><code>{{ job.target_module }}.{{ job.target_function }}</code></td>
            <td><span class="badge bg-secondary">{{ job.state_name }}</span></td>
            <td>{{ job.created_at }}</td>
        </tr>
        {% endfor %}
    </tbody>
</table>
<!-- Add pagination controls here -->
{% endblock %}
```

### 3. Expanded State Machine

#### 3.1. New States (`pytaskflow/common/states.py`)
Add `DeletedState` and `AwaitingState`. `AwaitingState` is a placeholder for Phase 4 (Continuations) but we'll define it now.

```python
# pytaskflow/common/states.py
# ...

class DeletedState(BaseState):
    NAME = "Deleted"
    
    def serialize_data(self) -> Dict[str, Any]:
        return {}

class AwaitingState(BaseState):
    NAME = "Awaiting"
    
    def __init__(self, parent_id: str, reason: str = None):
        super().__init__(reason)
        self.parent_id = parent_id

    def serialize_data(self) -> Dict[str, Any]:
        return {"parent_id": self.parent_id}
```

#### 3.2. Client Methods for New States
The `BackgroundJobClient` needs a method to delete jobs.

```python
# pytaskflow/client.py
from .common.states import DeletedState

class BackgroundJobClient:
    # ...
    def delete(self, job_id: str) -> bool:
        """Moves a job to the Deleted state."""
        return self.storage.set_job_state(job_id, DeletedState(reason="Manually deleted"))
```

### 4. Monitoring API in `JobStorage`

The dashboard relies on a rich monitoring API. We need to add these methods to the `JobStorage` ABC and implement them in `MemoryStorage` and `RedisStorage`.

```python
# pytaskflow/storage/base.py
class JobStorage(ABC):
    # ... existing methods ...
    @abstractmethod
    def get_statistics(self) -> dict:
        """Returns counts of jobs in various states."""
        ...
        
    @abstractmethod
    def get_jobs_by_state(self, state_name: str, start: int, count: int) -> List[Job]:
        """Fetches a paginated list of jobs in a specific state."""
        ...

    # Methods for worker monitoring
    @abstractmethod
    def server_heartbeat(self, server_id: str, worker_count: int, queues: List[str]) -> None: ...
    
    @abstractmethod
    def remove_server(self, server_id: str) -> None: ...
    
    @abstractmethod
    def get_servers(self) -> List[dict]: ...
```

#### 4.1. Implementation in `RedisStorage`

```python
# pytaskflow/storage/redis_storage.py
class RedisStorage(JobStorage):
    # ...
    def get_statistics(self) -> dict:
        # This is a simplified example. A real implementation would be more efficient,
        # possibly using dedicated counters.
        with self.redis_client.pipeline() as pipe:
            pipe.llen("pytaskflow:queue:default") # Simplified, should sum all queues
            pipe.zcard("pytaskflow:scheduled")
            pipe.llen("pytaskflow:queue:processing")
            pipe.scard("pytaskflow:sets:succeeded") # Assuming we add jobs to sets on completion
            pipe.scard("pytaskflow:sets:failed")
            results = pipe.execute()

        return {
            "enqueued": results[0],
            "scheduled": results[1],
            "processing": results[2],
            "succeeded": results[3],
            "failed": results[4],
        }
        
    def get_jobs_by_state(self, state_name: str, start: int, count: int) -> List[Job]:
        # This requires a new data structure: a Set for each state.
        # e.g., "pytaskflow:sets:succeeded"
        state_set_key = f"pytaskflow:sets:{state_name.lower()}"
        
        # SSCAN is preferable for large sets, but ZSCAN is easier for pagination
        # Let's use a List for simplicity in the MVP dashboard.
        # A more robust solution uses a Sorted Set scored by timestamp.
        job_ids = self.redis_client.lrange(state_set_key, start, start + count - 1)
        if not job_ids:
            return []
            
        with self.redis_client.pipeline() as pipe:
            for job_id in job_ids:
                pipe.hgetall(f"pytaskflow:job:{job_id.decode()}")
            
            job_data_list = pipe.execute()
            
        jobs = []
        for job_data in job_data_list:
             if job_data:
                decoded_data = {k.decode(): v.decode() for k, v in job_data.items()}
                jobs.append(Job(**decoded_data))
        return jobs

    def server_heartbeat(self, server_id: str, worker_count: int, queues: List[str]):
        server_key = f"pytaskflow:server:{server_id}"
        server_data = {
            "worker_count": worker_count,
            "queues": ",".join(queues),
            "heartbeat": datetime.utcnow().isoformat()
        }
        # Set with an expiry to automatically remove dead servers
        self.redis_client.hset(server_key, mapping=server_data)
        self.redis_client.expire(server_key, 60) # Server is considered dead after 60s
        self.redis_client.sadd("pytaskflow:servers", server_id)

    def remove_server(self, server_id: str):
        self.redis_client.srem("pytaskflow:servers", server_id)
        self.redis_client.delete(f"pytaskflow:server:{server_id}")
```

## IV. Summary of Phase 3 Deliverables

*   A `Worker` class that now uses a `ThreadPoolExecutor` to process I/O-bound jobs concurrently.
*   A basic, read-only dashboard built with FastAPI that can:
    *   Display high-level statistics (counts of jobs in different states).
    *   List jobs for each state in a paginated view.
    *   Show active servers/workers based on heartbeats.
*   The `JobStorage` interface and `RedisStorage` implementation are expanded with a comprehensive monitoring API.
*   The `DeletedState` and `AwaitingState` are added to the system, and the `BackgroundJobClient` has a corresponding `delete` method.
*   The worker now performs heartbeating, making its presence and status visible via the storage layer.

At the end of this phase, PyTaskFlow will not only be a robust and persistent job scheduler but will also provide the crucial visibility needed for real-world operations and debugging.