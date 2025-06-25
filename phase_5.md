# PyTaskFlow Implementation Plan - Phase 5: Polish, Performance, and Extensibility

## I. Introduction & Goals

**Phase 5** is the final push to mature PyTaskFlow into a production-grade, highly reliable, and extensible framework. The focus shifts from adding core features to refining existing ones, optimizing performance, and building out the ecosystem to encourage community contributions. This phase is about hardening the system, improving the developer experience, and ensuring long-term maintainability.

## II. Key Objectives

1.  **Job Continuations:** Implement the ability to chain jobs together, allowing a job to be enqueued only after its parent has successfully completed.
2.  **Performance Optimization:** Profile and optimize critical paths, especially in the storage backends and worker loop.
3.  **Advanced Filters:** Implement more of Hangfire's powerful built-in filters, such as `DisableConcurrentExecution`.
4.  **Extensibility Hooks:** Solidify the APIs for custom state handlers and other extension points.
5.  **Comprehensive Testing:** Achieve high test coverage, including integration and stress tests.
6.  **Packaging and Release:** Prepare for a `1.0` release by cleaning up the API, improving error messages, and finalizing packaging for PyPI.

## III. Detailed Breakdown & Implementation Steps

### 1. Job Continuations

This feature, a hallmark of Hangfire, allows for creating simple workflows. It relies on the `AwaitingState` defined in Phase 3.

#### 1.1. Client API Extension (`pytaskflow/client.py`)
Add a `continue_with` method to the client.

```python
# pytaskflow/client.py
from .common.states import AwaitingState

class BackgroundJobClient:
    # ... existing methods ...

    def continue_with(self, parent_job_id: str, target_func: Callable, *args: Any, **kwargs: Any) -> str:
        """
        Creates a continuation that will be executed only after the parent job has succeeded.
        """
        module_name = target_func.__module__
        func_name = target_func.__name__
        serialized_args, serialized_kwargs = self.serializer.serialize_args(target_func, *args, **kwargs)

        # The next state after the parent finishes will be Enqueued
        next_state = EnqueuedState()
        
        # The initial state of this new job is Awaiting
        awaiting_state = AwaitingState(parent_id=parent_job_id)

        job = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args,
            kwargs=serialized_kwargs,
            state_name=awaiting_state.name,
            state_data=awaiting_state.serialize_data()
        )
        
        # The job is created but not enqueued. It is tracked as a continuation.
        job_id = self.storage.add_continuation(parent_job_id, job)
        return job_id
```

#### 1.2. Storage Layer Support (`pytaskflow/storage/base.py` & `redis_storage.py`)
The storage needs to track continuations. A Redis **Set** is a good choice for this.

```python
# pytaskflow/storage/base.py
class JobStorage(ABC):
    # ...
    @abstractmethod
    def add_continuation(self, parent_job_id: str, continuation_job: Job) -> str: ...

    @abstractmethod
    def get_continuations(self, parent_job_id: str) -> List[str]: ...


# pytaskflow/storage/redis_storage.py
class RedisStorage(JobStorage):
    # ...
    def add_continuation(self, parent_job_id: str, continuation_job: Job) -> str:
        continuation_key = f"pytaskflow:job:{parent_job_id}:continuations"
        job_key = f"pytaskflow:job:{continuation_job.id}"

        with self.redis_client.pipeline() as pipe:
            # Create the continuation job itself in an Awaiting state
            pipe.hset(job_key, mapping=continuation_job.__dict__)
            
            # Add its ID to the parent's continuation set
            pipe.sadd(continuation_key, continuation_job.id)
            pipe.execute()
            
        return continuation_job.id

    def get_continuations(self, parent_job_id: str) -> List[str]:
        continuation_key = f"pytaskflow:job:{parent_job_id}:continuations"
        return [job_id.decode() for job_id in self.redis_client.smembers(continuation_key)]
```

#### 1.3. `JobProcessor` Logic Update (`pytaskflow/server/processor.py`)
When a job succeeds, the processor must trigger its continuations.

```python
# pytaskflow/server/processor.py
from ..common.states import SucceededState

class JobProcessor:
    def process(self):
        try:
            # ... perform the job ...
            succeeded_state = SucceededState(result=result)
            self.storage.set_job_state(self.job.id, succeeded_state, ...)
            
            # New: Trigger continuations on success
            self._trigger_continuations(self.job.id)

        except Exception as e:
            # ... handle failure ...
        finally:
            self.storage.acknowledge(self.job.id)
            
    def _trigger_continuations(self, parent_job_id: str):
        continuation_ids = self.storage.get_continuations(parent_job_id)
        logger.info(f"Job {parent_job_id} succeeded. Triggering {len(continuation_ids)} continuations.")
        
        for job_id in continuation_ids:
            # The client's requeue method can be adapted or a new internal method can be used
            # to move the job from Awaiting to Enqueued.
            job = self.storage.get_job_data(job_id)
            if job:
                enqueued_state = EnqueuedState(queue=job.queue, reason=f"Continuation of job {parent_job_id}")
                self.storage.set_job_state(job_id, enqueued_state, expected_old_state=AwaitingState.NAME)
```

### 2. Performance Optimizations

#### 2.1. Redis Pipelining
Review all `RedisStorage` methods to ensure multiple commands are bundled into pipelines wherever possible. This is especially important for complex operations like `add_recurring_job` and state transitions that update multiple keys.

#### 2.2. Efficient Dequeueing
The `BRPOPLPUSH` command is already efficient. For very high-throughput scenarios, consider allowing workers to fetch jobs in small batches instead of one by one to reduce network round-trips.

#### 2.3. Caching
*   **Dashboard:** Implement caching for statistics and job lists to reduce the load on the storage backend when many users are viewing the dashboard. A simple time-based cache (e.g., 5 seconds) is effective.
*   **Internal:** Cache schema-related information or other static data that is repeatedly fetched from storage.

### 3. Advanced Filters

#### 3.1. `DisableConcurrentExecution` Filter (`pytaskflow/filters/builtin.py`)
This filter prevents multiple instances of the same job (or jobs sharing a resource key) from running simultaneously. It requires a distributed lock.

```python
# pytaskflow/filters/builtin.py
from .base import JobFilter
from ..common.states import ScheduledState
from ..storage.base import JobStorage # To access the lock

class DisableConcurrentExecution(JobFilter):
    def __init__(self, resource_key: str, timeout_seconds: int = 60 * 5):
        self.resource_key = resource_key
        self.timeout = timedelta(seconds=timeout_seconds)

    def on_performing(self, performing_context):
        storage = performing_context.storage
        
        # The lock key can be formatted with job arguments
        formatted_key = self.resource_key.format(*performing_context.job.args, **performing_context.job.kwargs)
        lock_key = f"pytaskflow:lock:{formatted_key}"
        
        # Try to acquire the lock
        lock = storage.acquire_distributed_lock(lock_key, self.timeout)
        
        if lock is None:
            # Could not acquire lock, another job is running. Reschedule this one.
            performing_context.canceled = True
            performing_context.next_state = ScheduledState(
                enqueue_at=datetime.utcnow() + timedelta(seconds=60),
                reason=f"Could not acquire lock on resource: {formatted_key}"
            )
        else:
            # Lock acquired, store it in the context to be released after the job runs
            performing_context.items["distributed_lock"] = lock

    def on_performed(self, performed_context):
        # Release the lock if it was acquired
        if "distributed_lock" in performed_context.items:
            performed_context.items["distributed_lock"].release()
```
**Note:** This requires `acquire_distributed_lock` to be added to the `JobStorage` ABC. Redis implementation can use `SET ... NX EX ...`.

### 4. Extensibility Hooks

#### 4.1. State Handlers
Formalize the concept of state handlers, which are custom logic hooks that run when a job enters or leaves a specific state.

```python
# pytaskflow/storage/base.py
class JobStorage(ABC):
    # ...
    def get_state_handlers(self) -> List[Any]: # 'Any' is IStateHandler conceptually
        return []

# pytaskflow/server/processor.py -> set_job_state
def set_job_state(self, ...):
    # ...
    # Before setting the new state:
    old_state_handlers = self.storage.get_state_handlers_for(job.state_name)
    for handler in old_state_handlers:
        handler.on_unapply(...)
    
    # ... set new state ...
    
    # After setting the new state:
    new_state_handlers = self.storage.get_state_handlers_for(state.name)
    for handler in new_state_handlers:
        handler.on_apply(...)
```
This allows storage backends or plugins to add custom logic, like updating an external monitoring system or cleaning up resources when a job enters a final state.

### 5. Comprehensive Testing

*   **Unit Tests:** Ensure high coverage (>90%) for all core components, serializers, and filters. Use `pytest` and `pytest-mock`.
*   **Integration Tests:** Write tests for each storage backend (`MemoryStorage`, `RedisStorage`, and `SqlAlchemyStorage`) that run through the entire job lifecycle (enqueue, process, succeed, fail, retry, delete, schedule, continue).
*   **Concurrency Tests:** Create tests that run multiple workers concurrently to try and expose race conditions in the storage layer, especially around `dequeue` and locking.
*   **Stress Tests:** Write scripts that enqueue thousands of jobs in a short period to test the system's throughput and stability under load.
*   **CI/CD:** Use GitHub Actions to run the full test suite on every push and pull request against multiple Python versions.

### 6. Packaging and Release

#### 6.1. API Review and Cleanup
*   Go through the entire public API (`pytaskflow/__init__.py`, `BackgroundJobClient`, etc.) and ensure consistency, clarity, and good naming.
*   Mark internal or experimental modules/classes with a leading underscore (`_`) or by not exporting them in `__init__.py`.
*   Deprecate any methods that will be removed in a future version using `warnings.warn`.

#### 6.2. `pyproject.toml` Finalization
*   Set the version to `1.0.0`.
*   Define optional dependencies for each backend (e.g., `pytaskflow[redis]`, `pytaskflow[fastapi]`).
    ```toml
    [project.optional-dependencies]
    redis = ["redis"]
    sqlalchemy = ["sqlalchemy", "psycopg2-binary"] # Example
    fastapi = ["fastapi"]
    litestar = ["litestar"]
    ```
*   Ensure all classifiers, author details, and project URLs are correct.

#### 6.3. Release Process
*   Create a `CHANGELOG.md` file.
*   Tag the release in Git (e.g., `v1.0.0`).
*   Build the package using a tool like `hatch` or `build`.
*   Publish the package to PyPI using `twine`.

## IV. Summary of Phase 5 Deliverables

*   A fully functional job continuation system via the `client.continue_with` method.
*   Significant performance improvements in the Redis storage backend through optimized command pipelining.
*   New built-in filters like `DisableConcurrentExecution` that leverage a distributed locking mechanism.
*   A well-defined and documented system for state handlers, enabling deeper customization.
*   A robust test suite with high coverage, including integration and stress tests, ensuring the reliability of the `1.0` release.
*   Polished, well-documented public APIs.
*   The project is packaged and ready for a `1.0.0` release on PyPI, complete with optional dependencies for different backends and integrations.

Upon completion of Phase 5, PyTaskFlow will be a mature, feature-complete, and production-ready library that stands as a powerful and modern alternative in the Python background job processing landscape.