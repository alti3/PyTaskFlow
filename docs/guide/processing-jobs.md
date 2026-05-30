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

## Preventing Concurrent Execution

`DisableConcurrentExecution` can be passed to a worker to ensure jobs sharing a
resource key do not run at the same time:

```python
from pytaskflow.filters.builtin import DisableConcurrentExecution
from pytaskflow.server.worker import Worker

worker = Worker(
    storage,
    queues=["default"],
    filters=[DisableConcurrentExecution("customer-import:{customer_id}")],
)
```

If the lock is already held, the job is moved back to `Scheduled` and retried
after the filter's retry delay.
