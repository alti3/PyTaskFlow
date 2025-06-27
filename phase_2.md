# PyTaskFlow Implementation Plan - Phase 2: Persistence and Advanced Job Types

## I. Introduction & Goals

**Phase 2** transitions PyTaskFlow from an in-memory MVP to a robust, persistent background job framework. The primary goal is to introduce a production-ready storage backend using Redis, which is a popular choice for this kind of workload due to its speed and versatile data structures. This phase also introduces two critical features inspired by Hangfire: **delayed jobs** and **recurring jobs**, making the system significantly more powerful for real-world applications. Finally, we will integrate basic, structured logging to improve observability.

## II. Key Objectives

1.  **Redis Storage:** Implement the `JobStorage` interface using Redis as the persistence layer.
2.  **Delayed Jobs:** Introduce a new `Scheduled` state and a scheduler process to enqueue jobs after a specified delay.
3.  **Recurring Jobs:** Implement functionality to schedule jobs based on CRON expressions.
4.  **Basic Logging:** Integrate Python's standard `logging` module throughout the core components.

## III. Detailed Breakdown & Implementation Steps

### 1. Redis Storage Implementation (`pytaskflow/storage/redis_storage.py`)

This is the most critical component of Phase 2. We will leverage `redis-py` and its atomic operations to build a reliable storage backend.

#### 1.1. Data Structure Mapping
We will map PyTaskFlow concepts to Redis data structures. All keys should be prefixed (e.g., `pytaskflow:`).

*   **Job Data:** A Redis **Hash** for each job.
    *   Key: `pytaskflow:job:{job_id}`
    *   Fields: All fields from the `Job` dataclass (`target_module`, `args`, `state_name`, etc.).
*   **Queues:** A Redis **List** for each queue.
    *   Key: `pytaskflow:queue:{queue_name}`
    *   Operation: `LPUSH` to enqueue, `BRPOPLPUSH` to atomically dequeue and place into a processing list.
*   **Processing List:** A Redis **List** per queue for workers to safely hold jobs they are processing. This is crucial for requeueing jobs if a worker crashes.
    *   Key: `pytaskflow:queue:{queue_name}:processing`
*   **State History:** A Redis **List** for each job's state transitions.
    *   Key: `pytaskflow:job:{job_id}:history`
    *   Content: Serialized state data (e.g., JSON string of `{"state": "Succeeded", "timestamp": "...", "data": {...}}`).
*   **Scheduled Jobs:** A Redis **Sorted Set**.
    *   Key: `pytaskflow:scheduled`
    *   Score: The UNIX timestamp (float) when the job should be enqueued.
    *   Value: The `job_id`.
*   **Recurring Jobs:**
    *   A Redis **Hash** to store all recurring job definitions.
        *   Key: `pytaskflow:recurring-jobs`
        *   Field: `recurring_job_id`
        *   Value: Serialized JSON containing the job template, CRON expression, timezone, etc.
    *   A Redis **Set** to track all recurring job IDs.
        *   Key: `pytaskflow:recurring-jobs:ids`
*   **Statistics:** Redis **Hashes** or simple **Keys**.
    *   Key: `pytaskflow:stats` (Hash) or `pytaskflow:stats:succeeded` (Key)
    *   Operation: Use `INCR` and `DECR` for atomic counter updates.

#### 1.2. `RedisStorage` Class Implementation
This class will inherit from `JobStorage` and implement all its abstract methods.

```python
# pytaskflow/storage/redis_storage.py
import redis
import json
from typing import Optional, List

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState

class RedisStorage(JobStorage):
    def __init__(self, connection_pool: Optional[redis.ConnectionPool] = None, **redis_options):
        if connection_pool:
            self.redis_client = redis.Redis(connection_pool=connection_pool)
        else:
            self.redis_client = redis.Redis(**redis_options)
        # Lua script for atomically moving a job from scheduled to enqueued
        self.move_to_enqueued_script = self.redis_client.script_load("""
            local job_id = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)[1]
            if job_id then
                redis.call('zrem', KEYS[1], job_id)
                redis.call('hset', 'pytaskflow:job:' .. job_id, 'state_name', ARGV[2])
                redis.call('lpush', 'pytaskflow:queue:' .. ARGV[3], job_id)
                return job_id
            end
            return nil
        """)

    def enqueue(self, job: Job) -> str:
        with self.redis_client.pipeline() as pipe:
            job_key = f"pytaskflow:job:{job.id}"
            # Using __dict__ from the dataclass for simplicity
            pipe.hset(job_key, mapping=job.__dict__)
            pipe.lpush(f"pytaskflow:queue:{job.queue}", job.id)
            pipe.execute()
        return job.id
    
    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        # BRPOPLPUSH is atomic and reliable. It moves an item from one list to another.
        # If the worker crashes, the job remains in the processing list.
        queue_keys = [f"pytaskflow:queue:{q}" for q in queues]
        
        # This command blocks until a job is available or the timeout is reached.
        job_id = self.redis_client.brpoplpush(queue_keys, "pytaskflow:queue:processing:temp", timeout=timeout_seconds)
        
        if not job_id:
            return None
        
        job_id = job_id.decode() # redis-py returns bytes
        job_key = f"pytaskflow:job:{job_id}"
        
        # Rename the temporary processing item to a worker-specific one
        # This will be refined in later phases. For now, it's just 'processing'.
        self.redis_client.lrem("pytaskflow:queue:processing:temp", 1, job_id)
        self.redis_client.lpush(f"pytaskflow:queue:processing", job_id)

        job_data = self.redis_client.hgetall(job_key)
        
        if not job_data:
            # Job data expired or was deleted between dequeue and fetch
            self.redis_client.lrem(f"pytaskflow:queue:processing", 1, job_id)
            return None
            
        # Decode bytes from redis
        job_dict = {k.decode(): v.decode() for k, v in job_data.items()}
        
        # Update state to Processing
        self.set_job_state(job_id, ProcessingState("server-redis", "worker-1"))
        
        return Job(**job_dict)

    def acknowledge(self, job_id: str) -> None:
        # Remove from the processing list
        self.redis_client.lrem(f"pytaskflow:queue:processing", 1, job_id)

    # ... implement other abstract methods ...
```

### 2. Delayed Jobs & Scheduler

#### 2.1. `ScheduledState`
Add the new state to `pytaskflow/common/states.py`.

```python
# pytaskflow/common/states.py

# ... other states ...

class ScheduledState(BaseState):
    NAME = "Scheduled"
    
    def __init__(self, enqueue_at: datetime, scheduled_at: datetime, reason: str = None):
        super().__init__(reason)
        self.enqueue_at = enqueue_at
        self.scheduled_at = scheduled_at

    def serialize_data(self) -> Dict[str, Any]:
        return {
            "enqueue_at": self.enqueue_at.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat(),
        }
```

#### 2.2. Client and Storage Methods
Update `BackgroundJobClient` and `JobStorage` to handle scheduling.

```python
# pytaskflow/client.py
class BackgroundJobClient:
    # ... existing methods ...
    def schedule(self, target_func: Callable, enqueue_at: datetime, *args: Any, **kwargs: Any) -> str:
        # Similar to enqueue, but creates a Job with ScheduledState
        module_name = target_func.__module__
        func_name = target_func.__name__
        serialized_args = self.serializer.serialize_args(target_func, *args, **kwargs)
        
        scheduled_state = ScheduledState(enqueue_at, datetime.now(UTC))

        job = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args[0],
            kwargs=serialized_args[1],
            state_name=scheduled_state.name,
            state_data=scheduled_state.serialize_data()
        )
        
        job_id = self.storage.schedule(job, enqueue_at)
        return job_id

# pytaskflow/storage/base.py
class JobStorage(ABC):
    # ... existing methods ...
    @abstractmethod
    def schedule(self, job: Job, enqueue_at: datetime) -> str: ...

# pytaskflow/storage/redis_storage.py
class RedisStorage(JobStorage):
    # ... existing methods ...
    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        job_key = f"pytaskflow:job:{job.id}"
        scheduled_key = "pytaskflow:scheduled"
        
        # Convert datetime to UNIX timestamp for the score
        score = enqueue_at.timestamp()
        
        with self.redis_client.pipeline() as pipe:
            pipe.hset(job_key, mapping=job.__dict__)
            pipe.zadd(scheduled_key, {job.id: score})
            pipe.execute()
        return job.id
```

#### 2.3. The `DelayedJobScheduler` Process
This logic will be integrated into the `Worker`'s main loop. It's a separate logical component that runs periodically.

```python
# pytaskflow/server/worker.py

# ... imports ...
from ..common.states import EnqueuedState

class Worker:
    def __init__(self, ..., scheduler_poll_interval_seconds: int = 15):
        # ...
        self.scheduler_poll_interval = scheduler_poll_interval_seconds
        self._last_scheduler_run = 0

    def _run_schedulers(self):
        """Runs periodic tasks like enqueuing scheduled jobs."""
        now = time.monotonic()
        if (now - self._last_scheduler_run) < self.scheduler_poll_interval:
            return
        
        # This should be wrapped in a distributed lock to prevent multiple workers from running it
        # but for simplicity in this phase, we omit the lock.
        print(f"[{self.worker_id}] Running schedulers...")
        self._enqueue_due_scheduled_jobs()
        # self._enqueue_due_recurring_jobs() # Added in next step
        
        self._last_scheduler_run = now

    def _enqueue_due_scheduled_jobs(self):
        storage = self.storage
        if not isinstance(storage, RedisStorage): # Phase 2 only supports Redis
            return

        now_timestamp = datetime.now(UTC).timestamp()
        
        while True:
            # Using the Lua script to atomically move one job
            job_id_bytes = storage.move_to_enqueued_script(
                keys=["pytaskflow:scheduled"], 
                args=[now_timestamp, EnqueuedState.NAME, "default"] # Assume default queue for now
            )
            
            if not job_id_bytes:
                break # No more jobs to enqueue
            
            print(f"[{self.worker_id}] Moved scheduled job {job_id_bytes.decode()} to enqueued.")


    def run(self):
        """Updated worker loop."""
        print(f"[{self.worker_id}] Starting worker...")
        while not self._shutdown_requested:
            self._run_schedulers()
            
            # Dequeue logic remains the same
            job = self.storage.dequeue(self.queues, timeout_seconds=1)
            # ... process job if found ...
```

### 3. Recurring Jobs & Scheduler

#### 3.1. CRON Library
Add `cronsim` to the project's dependencies in `pyproject.toml`.
`dependencies = [ ..., "cronsim" ]`

#### 3.2. Client and Storage Methods for Recurring Jobs
These methods allow for the management of recurring jobs.

```python
# pytaskflow/client.py
class BackgroundJobClient:
    # ...
    def add_or_update_recurring(self, recurring_job_id: str, target_func: Callable, cron_expression: str, *args, **kwargs):
        # Creates a job template (without an ID) to be stored.
        job_template = Job(...)
        self.storage.add_recurring_job(recurring_job_id, job_template, cron_expression)

    def remove_recurring_if_exists(self, recurring_job_id: str):
        self.storage.remove_recurring_job(recurring_job_id)

    def trigger(self, recurring_job_id: str):
        self.storage.trigger_recurring_job(recurring_job_id)

# pytaskflow/storage/base.py
class JobStorage(ABC):
    # ...
    @abstractmethod
    def add_recurring_job(self, recurring_job_id: str, job_template: Job, cron_expression: str): ...
    
    @abstractmethod
    def remove_recurring_job(self, recurring_job_id: str): ...
    
    @abstractmethod
    def trigger_recurring_job(self, recurring_job_id: str): ...
    
    # ... also need methods to get/update last execution time etc.

# pytaskflow/storage/redis_storage.py
class RedisStorage(JobStorage):
    # ...
    def add_recurring_job(self, recurring_job_id: str, job_template: Job, cron_expression: str):
        # Serialize the entire job definition and options into a JSON string
        data = {
            "job": job_template.__dict__,
            "cron": cron_expression,
            "last_execution": None # Or current time
        }
        self.redis_client.hset("pytaskflow:recurring-jobs", recurring_job_id, json.dumps(data))
        self.redis_client.sadd("pytaskflow:recurring-jobs:ids", recurring_job_id)

    # ... implement other recurring job methods ...
```

#### 3.3. The `RecurringJobScheduler` Process
This logic is more complex and also integrated into the worker's scheduler run.

```python
# pytaskflow/server/worker.py
from consim import CronSim

class Worker:
    # ...
    def _run_schedulers(self):
        # ...
        self._enqueue_due_recurring_jobs()
        # ...
    
    def _enqueue_due_recurring_jobs(self):
        # This entire method should be protected by a distributed lock to ensure only one worker
        # runs the recurring job scheduler at a time.
        lock_key = "pytaskflow:lock:recurring-scheduler"
        if not self.storage.redis_client.set(lock_key, self.server_id, ex=60, nx=True):
            return # Another worker is handling it

        try:
            now = datetime.now(UTC)
            recurring_job_ids = self.storage.redis_client.smembers("pytaskflow:recurring-jobs:ids")
            
            for job_id_bytes in recurring_job_ids:
                job_id = job_id_bytes.decode()
                
                # Use a distributed lock per job to handle updates atomically
                job_lock_key = f"pytaskflow:lock:recurring-job:{job_id}"
                if not self.storage.redis_client.set(job_lock_key, self.server_id, ex=10, nx=True):
                    continue

                try:
                    data_str = self.storage.redis_client.hget("pytaskflow:recurring-jobs", job_id)
                    if not data_str:
                        continue
                        
                    data = json.loads(data_str.decode())
                    
                    last_execution_str = data.get("last_execution")
                    last_execution = datetime.fromisoformat(last_execution_str) if last_execution_str else now
                    
                    cron = CronSim(data["cron"], last_execution)
                    next_execution = next(cron)
                    
                    if next_execution <= now:
                        # It's time to run!
                        print(f"[{self.worker_id}] Triggering recurring job {job_id}")
                        job_dict = data["job"]
                        # Create a new, unique job instance from the template
                        job_instance = Job(
                            target_module=job_dict["target_module"],
                            target_function=job_dict["target_function"],
                            args=job_dict["args"],
                            kwargs=job_dict["kwargs"],
                            state_name=EnqueuedState.NAME,
                            state_data=EnqueuedState().serialize_data(),
                            queue=job_dict.get("queue", "default"),
                            # recurring_job_id=job_id # Link it back
                        )
                        self.storage.enqueue(job_instance)
                        
                        # Update last execution time
                        data["last_execution"] = now.isoformat()
                        self.storage.redis_client.hset("pytaskflow:recurring-jobs", job_id, json.dumps(data))
                finally:
                    self.storage.redis_client.delete(job_lock_key)

        finally:
            self.storage.redis_client.delete(lock_key)
```

### 4. Basic Logging

Integrate Python's `logging` module.

```python
# pytaskflow/server/worker.py
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Worker:
    # ...
    def run(self):
        logger.info(f"[{self.worker_id}] Starting worker for queues: {', '.join(self.queues)}")
        while not self._shutdown_requested:
            # ...
                if job:
                    logger.info(f"Processing job {job.id} from queue '{job.queue}'...")
                    # ...
                    logger.info(f"Finished job {job.id}")
                # ...
        logger.info(f"[{self.worker_id}] Worker has stopped.")

# pytaskflow/server/processor.py
import logging
logger = logging.getLogger(__name__)

class JobProcessor:
    # ...
    def process(self):
        try:
            # ...
        except Exception as e:
            logger.error(f"Job {self.job.id} failed.", exc_info=True)
            # ...
```

## IV. Summary of Phase 2 Deliverables

*   A fully functional `RedisStorage` class implementing the `JobStorage` contract.
*   The `BackgroundJobClient` now supports a `schedule` method for delayed jobs.
*   The `BackgroundJobClient` now has methods to `add_or_update_recurring`, `remove_recurring_if_exists`, and `trigger` recurring jobs.
*   A `ScheduledState` class is added to the state machine.
*   The `Worker` process is enhanced with two scheduler components: one for delayed jobs and one for recurring jobs, both using Redis as their backend.
*   Structured logging is added to key components, improving the traceability and debuggability of the system.
*   The project now requires `redis-py` and `cronsim` as dependencies.

With the completion of Phase 2, PyTaskFlow will be a feature-rich and persistent background job system ready for more advanced capabilities.