# Processing Jobs

## Threaded Worker

```python
from pytaskflow.server.worker import Worker

worker = Worker(storage, queues=["default"], worker_count=4)
worker.run()
```

## Asyncio Worker

Use asyncio mode to run async job functions directly:

```python
from pytaskflow.server.worker import Worker, ConcurrencyMode

worker = Worker(
    storage,
    queues=["default"],
    worker_count=10,
    concurrency_mode=ConcurrencyMode.ASYNCIO,
)
worker.run()
```

When using asyncio mode, synchronous jobs are automatically offloaded to a thread.
