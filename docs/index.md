# PyTaskFlow

PyTaskFlow is a Python background job framework that provides reliable job execution, recurring scheduling, and a clean operational dashboard. It supports Redis-backed persistence and a flexible worker that can run sync or async workloads.

## Highlights

- Fire-and-forget jobs with retries
- Delayed and recurring jobs
- Redis-backed persistence
- Threaded or asyncio worker execution
- Read-only dashboard with job visibility and retry/delete actions

## Quick Start

```bash
uv add pytaskflow
```

```python
from pytaskflow.client import BackgroundJobClient
from pytaskflow.storage.redis_storage import RedisStorage

storage = RedisStorage()
client = BackgroundJobClient(storage)

# Enqueue a job
client.enqueue(my_task, 1, 2)
```

To process jobs, run a worker:

```python
from pytaskflow.server.worker import Worker

worker = Worker(storage, queues=["default"], worker_count=4)
worker.run()
```
