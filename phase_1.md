# PyTaskFlow Implementation Plan - Phase 1: Core MVP

## I. Introduction & Goals

**Phase 1** focuses on building the Minimum Viable Product (MVP) for PyTaskFlow. The primary goal is to establish a solid, in-memory, single-threaded foundation that proves the core concepts of job creation, queuing, and execution. This phase prioritizes correctness and a clear, extensible architecture over performance and advanced features. By the end of this phase, a developer should be able to enqueue a Python function, have a worker pick it up and execute it, and see its state change from `Enqueued` to `Processing` and finally to `Succeeded` or `Failed`.

## II. Key Objectives

1.  **Core Data Models:** Define the `Job` and `State` classes.
2.  **Serialization:** Implement a default JSON-based serializer for jobs and their arguments.
3.  **Storage:** Create the `JobStorage` abstract base class and a simple, thread-safe `MemoryStorage` implementation.
4.  **Client:** Develop a basic `BackgroundJobClient` capable of creating fire-and-forget jobs.
5.  **Worker:** Build a single-threaded, synchronous `Worker` that can fetch and process jobs from a queue.
6.  **Filters:** Establish the job filter pipeline and implement a basic automatic retry filter.
7.  **Configuration:** A simple mechanism to set the global storage.

## III. Detailed Breakdown & Implementation Steps

### 1. Common Data Models (`pytaskflow/common/`)

#### 1.1. The `Job` Dataclass (`pytaskflow/common/job.py`)
This will be a simple `dataclass` to hold all information required to execute a job. It is designed to be easily serializable.

```python
# pytaskflow/common/job.py
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

@dataclass
class Job:
    """
    Represents a unit of work to be executed in the background.
    
    This is the central data model that gets stored and passed around.
    Inspired by Hangfire.Common.Job.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Target function information
    target_module: str
    target_function: str
    
    # Serialized arguments
    args: str  # JSON-serialized tuple
    kwargs: str  # JSON-serialized dict

    # State information
    state_name: str
    state_data: Dict[str, Any] = field(default_factory=dict)
    
    # Job metadata
    queue: str = "default"
    retry_count: int = 0
    
    # For future phases
    # scheduled_at: Optional[datetime] = None
    # recurring_job_id: Optional[str] = None
```

**Implementation Notes:**
*   Using `dataclasses` provides `__init__`, `__repr__`, etc., for free.
*   Arguments (`args`, `kwargs`) are stored as serialized strings to ensure they are persistence-agnostic. The `Serializer` will handle their conversion.
*   The `target_module` and `target_function` strings are the key to deserializing the job call, avoiding the pitfalls of pickling function objects.

#### 1.2. State Definitions (`pytaskflow/common/states.py`)
We will create a base class for all states and the four initial states required for the MVP. This mirrors Hangfire's `IState` interface and concrete state classes.

```python
# pytaskflow/common/states.py
from abc import ABC, abstractmethod
from typing import Dict, Any, final

class BaseState(ABC):
    """
    Abstract base class for all job states.
    Inspired by Hangfire.States.IState.
    """
    NAME: str = "State"

    def __init__(self, reason: str = None):
        self.reason = reason

    @final
    @property
    def name(self) -> str:
        return self.NAME

    @abstractmethod
    def serialize_data(self) -> Dict[str, Any]:
        """Serializes state-specific data into a dictionary."""
        raise NotImplementedError

class EnqueuedState(BaseState):
    NAME = "Enqueued"
    
    def __init__(self, queue: str = "default", reason: str = None):
        super().__init__(reason)
        self.queue = queue

    def serialize_data(self) -> Dict[str, Any]:
        return {"queue": self.queue}

class ProcessingState(BaseState):
    NAME = "Processing"

    def __init__(self, server_id: str, worker_id: str, reason: str = None):
        super().__init__(reason)
        self.server_id = server_id
        self.worker_id = worker_id
        
    def serialize_data(self) -> Dict[str, Any]:
        return {"server_id": self.server_id, "worker_id": self.worker_id}

class SucceededState(BaseState):
    NAME = "Succeeded"
    
    def __init__(self, result: Any, reason: str = None):
        super().__init__(reason)
        self.result = result # Note: result must be serializable
        
    def serialize_data(self) -> Dict[str, Any]:
        # The serializer will handle the result object
        return {"result": self.result}

class FailedState(BaseState):
    NAME = "Failed"

    def __init__(self, exception_type: str, exception_message: str, exception_details: str, reason: str = None):
        super().__init__(reason)
        self.exception_type = exception_type
        self.exception_message = exception_message
        self.exception_details = exception_details # Full traceback

    def serialize_data(self) -> Dict[str, Any]:
        return {
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "exception_details": self.exception_details,
        }
```

#### 1.3. Custom Exceptions (`pytaskflow/common/exceptions.py`)
This file will house custom exceptions for clearer error handling, similar to `Hangfire.Core/Common/JobLoadException.cs`.

```python
# pytaskflow/common/exceptions.py

class PyTaskFlowException(Exception):
    """Base exception for the PyTaskFlow library."""
    pass

class JobLoadError(PyTaskFlowException):
    """Raised when a job's target function cannot be loaded."""
    pass
```

### 2. Serialization (`pytaskflow/serialization/`)

#### 2.1. Base Serializer ABC (`pytaskflow/serialization/base.py`)
Defines the contract for all serializers.

```python
# pytaskflow/serialization/base.py
from abc import ABC, abstractmethod
from typing import Callable, Tuple, Dict, Any

from pytaskflow.common.job import Job

class BaseSerializer(ABC):
    @abstractmethod
    def serialize_job(self, job: Job) -> str: ...

    @abstractmethod
    def deserialize_job(self, data: str) -> Job: ...

    @abstractmethod
    def serialize_args(self, target_func: Callable, *args: Any, **kwargs: Any) -> Tuple[str, str]: ...

    @abstractmethod
    def deserialize_args(self, args_str: str, kwargs_str: str) -> Tuple[Tuple, Dict]: ...

    @abstractmethod
    def serialize_state_data(self, data: Dict[str, Any]) -> str: ...

    @abstractmethod
    def deserialize_state_data(self, data_str: str) -> Dict[str, Any]: ...
```

#### 2.2. JSON Serializer (`pytaskflow/serialization/json_serializer.py`)
The default implementation using Python's standard `json` library.

```python
# pytaskflow/serialization/json_serializer.py
import json
from typing import Callable, Tuple, Dict, Any

from .base import BaseSerializer
from ..common.job import Job

class JsonSerializer(BaseSerializer):
    def serialize_job(self, job: Job) -> str:
        # For Phase 1, we can just serialize the dataclass. Later, this might get more complex.
        # This assumes state_data is already prepared for JSON.
        return json.dumps(job.__dict__)

    def deserialize_job(self, data: str) -> Job:
        job_dict = json.loads(data)
        return Job(**job_dict)

    def serialize_args(self, target_func: Callable, *args: Any, **kwargs: Any) -> Tuple[str, str]:
        # This method is more for the client-side to prepare the job
        # For now, let's assume the client prepares serialized args directly.
        # This method will become more important with decorators.
        return json.dumps(args), json.dumps(kwargs)

    def deserialize_args(self, args_str: str, kwargs_str: str) -> Tuple[Tuple, Dict]:
        return tuple(json.loads(args_str)), json.loads(kwargs_str)
    
    def serialize_state_data(self, data: Dict[str, Any]) -> str:
        # Special handling for non-serializable objects like 'result' in SucceededState might be needed
        # For MVP, we'll assume basic types.
        return json.dumps(data)

    def deserialize_state_data(self, data_str: str) -> Dict[str, Any]:
        return json.loads(data_str)
```

### 3. Storage Layer (`pytaskflow/storage/`)

#### 3.1. `JobStorage` ABC (`pytaskflow/storage/base.py`)
The abstract base class defines the contract for all storage backends. This is analogous to `Hangfire.Core/Storage/IStorageConnection.cs` and `Hangfire.Core/Storage/IWriteOnlyTransaction.cs` combined for simplicity in Python.

```python
# pytaskflow/storage/base.py
from abc import ABC, abstractmethod
from typing import Optional, List

from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState

class JobStorage(ABC):
    @abstractmethod
    def enqueue(self, job: Job) -> str: ...

    @abstractmethod
    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]: ...

    @abstractmethod
    def acknowledge(self, job_id: str) -> None: ...
    
    @abstractmethod
    def set_job_state(self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None) -> bool: ...

    @abstractmethod
    def get_job_data(self, job_id: str) -> Optional[Job]: ...
```
**Note:** In Phase 1, `dequeue` will be blocking. We'll introduce `timeout` to avoid permanent blocks.

#### 3.2. `MemoryStorage` (`pytaskflow/storage/memory_storage.py`)
A simple in-memory implementation for testing and development. It must be thread-safe.

```python
# pytaskflow/storage/memory_storage.py
import time
from collections import deque
from threading import Lock, Condition
from typing import Optional, List, Dict

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState

class MemoryStorage(JobStorage):
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._queues: Dict[str, deque[str]] = {}
        self._processing: Dict[str, Job] = {} # Jobs currently being processed
        self._lock = Lock()
        self._condition = Condition(self._lock)

    def enqueue(self, job: Job) -> str:
        with self._lock:
            self._jobs[job.id] = job
            if job.queue not in self._queues:
                self._queues[job.queue] = deque()
            self._queues[job.queue].append(job.id)
            self._condition.notify() # Notify any waiting worker
        return job.id

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        with self._lock:
            start_time = time.monotonic()
            while True:
                for queue_name in queues:
                    if queue_name in self._queues and self._queues[queue_name]:
                        job_id = self._queues[queue_name].popleft()
                        job = self._jobs[job_id]
                        
                        # Atomically move to processing
                        self.set_job_state(job.id, ProcessingState("server-mvp", "worker-1"))
                        self._processing[job.id] = job
                        return job

                # Wait for a job to be enqueued or timeout
                remaining_timeout = timeout_seconds - (time.monotonic() - start_time)
                if remaining_timeout <= 0:
                    return None
                
                self._condition.wait(timeout=remaining_timeout)


    def acknowledge(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._processing:
                del self._processing[job_id]

    def set_job_state(self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None) -> bool:
        with self._lock:
            if job_id not in self._jobs:
                return False
            
            job = self._jobs[job_id]
            if expected_old_state and job.state_name != expected_old_state:
                return False
            
            job.state_name = state.name
            job.state_data = state.serialize_data()
            return True

    def get_job_data(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)
```

### 4. Client API (`pytaskflow/client.py` and `pytaskflow/config.py`)

#### 4.1. Global Configuration (`pytaskflow/config.py`)
A simple singleton-like object to hold the configured storage instance.

```python
# pytaskflow/config.py
from typing import Optional
from .storage.base import JobStorage

class _GlobalConfig:
    def __init__(self):
        self.storage: Optional[JobStorage] = None

_GLOBAL_CONFIG = _GlobalConfig()

def configure(storage: JobStorage) -> None:
    _GLOBAL_CONFIG.storage = storage

def get_storage() -> JobStorage:
    if not _GLOBAL_CONFIG.storage:
        raise RuntimeError("PyTaskFlow has not been configured. Call pytaskflow.configure() first.")
    return _GLOBAL_CONFIG.storage
```

#### 4.2. `BackgroundJobClient` (`pytaskflow/client.py`)
The user-facing API to create jobs.

```python
# pytaskflow/client.py
import importlib
from typing import Callable, Any, Optional

from .common.job import Job
from .common.states import EnqueuedState
from .storage.base import JobStorage
from .serialization.base import BaseSerializer
from .serialization.json_serializer import JsonSerializer

class BackgroundJobClient:
    def __init__(self, storage: JobStorage, serializer: Optional[BaseSerializer] = None):
        self.storage = storage
        self.serializer = serializer or JsonSerializer()

    def enqueue(self, target_func: Callable, *args: Any, **kwargs: Any) -> str:
        """Creates a fire-and-forget job."""
        
        # 1. Get function location
        module_name = target_func.__module__
        func_name = target_func.__name__
        
        # 2. Serialize arguments
        # For MVP, we'll let the user ensure args are JSON-serializable
        serialized_args = self.serializer.serialize_args(target_func, *args, **kwargs)

        # 3. Create Job object
        job = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args[0],
            kwargs=serialized_args[1],
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data()
        )
        
        # 4. Enqueue it
        job_id = self.storage.enqueue(job)
        return job_id
```

### 5. Server/Worker (`pytaskflow/server/` and `pytaskflow/execution/`)

#### 5.1. Job Performer (`pytaskflow/execution/performer.py`)
This module is responsible for the actual dynamic import and execution of the job function.

```python
# pytaskflow/execution/performer.py
import importlib

def perform_job(module_name: str, func_name: str, args: tuple, kwargs: dict) -> Any:
    """Dynamically imports and executes the target job function."""
    try:
        module = importlib.import_module(module_name)
        target_func = getattr(module, func_name)
        return target_func(*args, **kwargs)
    except (ImportError, AttributeError) as e:
        raise JobLoadError(f"Could not load job target: {module_name}.{func_name}") from e
```

#### 5.2. Job Processor (`pytaskflow/server/processor.py`)
A class that orchestrates the processing of a single job, including state changes and error handling.

```python
# pytaskflow/server/processor.py
import traceback
from .context import PerformContext
from ..common.job import Job
from ..common.states import SucceededState, FailedState
from ..execution.performer import perform_job
from ..storage.base import JobStorage
from ..serialization.base import BaseSerializer

class JobProcessor:
    def __init__(self, job: Job, storage: JobStorage, serializer: BaseSerializer):
        self.job = job
        self.storage = storage
        self.serializer = serializer

    def process(self):
        try:
            # 1. Deserialize args
            args, kwargs = self.serializer.deserialize_args(self.job.args, self.job.kwargs)
            
            # 2. Perform the job
            result = perform_job(self.job.target_module, self.job.target_function, args, kwargs)
            
            # 3. Set succeeded state
            succeeded_state = SucceededState(result=result, reason="Job performed successfully")
            self.storage.set_job_state(self.job.id, succeeded_state, expected_old_state=ProcessingState.NAME)

        except Exception as e:
            # 4. Handle failure
            exc_type = type(e).__name__
            exc_msg = str(e)
            exc_details = traceback.format_exc()
            
            failed_state = FailedState(
                exception_type=exc_type, 
                exception_message=exc_msg, 
                exception_details=exc_details
            )
            self.storage.set_job_state(self.job.id, failed_state, expected_old_state=ProcessingState.NAME)
        
        finally:
            # 5. Acknowledge completion
            self.storage.acknowledge(self.job.id)
```

#### 5.3. Worker (`pytaskflow/server/worker.py`)
The main worker process. In Phase 1, it's a simple, single-threaded loop.

```python
# pytaskflow/server/worker.py
import time
import uuid
from typing import List

from ..storage.base import JobStorage
from ..serialization.base import BaseSerializer
from .processor import JobProcessor

class Worker:
    def __init__(self, storage: JobStorage, serializer: BaseSerializer, queues: List[str] = ["default"]):
        self.storage = storage
        self.serializer = serializer
        self.queues = queues
        self.server_id = f"server:{uuid.uuid4()}"
        self.worker_id = f"worker:{uuid.uuid4()}"
        self._shutdown_requested = False

    def run(self):
        """Starts the worker's processing loop."""
        print(f"[{self.worker_id}] Starting worker for queues: {', '.join(self.queues)}")
        while not self._shutdown_requested:
            try:
                # 1. Dequeue a job
                job = self.storage.dequeue(self.queues, timeout_seconds=1)
                
                if job:
                    print(f"[{self.worker_id}] Picked up job {job.id}")
                    # 2. Process it
                    processor = JobProcessor(job, self.storage, self.serializer)
                    processor.process()
                    print(f"[{self.worker_id}] Finished processing job {job.id}")
                else:
                    # No job found, wait a bit before polling again
                    time.sleep(1)

            except KeyboardInterrupt:
                print(f"[{self.worker_id}] Shutdown requested...")
                self._shutdown_requested = True
            except Exception as e:
                print(f"[{self.worker_id}] Unhandled exception in worker loop: {e}")
                time.sleep(5) # Cooldown period after a major failure
        
        print(f"[{self.worker_id}] Worker has stopped.")
```

### 6. Job Filters (`pytaskflow/filters/`)

#### 6.1. Filter Base ABC (`pytaskflow/filters/base.py`)
This defines the interface for all filters. We'll only implement `on_state_election` for now.

```python
# pytaskflow/filters/base.py
from abc import ABC, abstractmethod

class JobFilter(ABC):
    def on_state_election(self, elect_state_context):
        pass # Default implementation does nothing
```

#### 6.2. `RetryAttribute` Filter (`pytaskflow/filters/builtin.py`)
This is a conceptual name. In Python, it will be a class. This filter implements the automatic retry logic.

```python
# pytaskflow/filters/builtin.py
from .base import JobFilter
from ..common.states import EnqueuedState, FailedState

class RetryFilter(JobFilter):
    def __init__(self, attempts: int = 3):
        self.attempts = attempts

    def on_state_election(self, elect_state_context):
        # This context object will be fleshed out in a later phase.
        # For now, let's assume it has these properties.
        job = elect_state_context.job
        candidate_state = elect_state_context.candidate_state
        
        if isinstance(candidate_state, FailedState):
            current_retry_count = job.retry_count
            
            if current_retry_count < self.attempts:
                # Increment retry count and re-enqueue
                job.retry_count += 1
                
                # Change the candidate state to Enqueued
                new_reason = f"Retrying job... Attempt {job.retry_count} of {self.attempts}"
                elect_state_context.candidate_state = EnqueuedState(
                    queue=job.queue, 
                    reason=new_reason
                )
```
**Note:** The filter system itself (how filters are discovered and applied) is not fully built in this phase but the core retry logic is defined. For the MVP, the `JobProcessor` could manually apply a `RetryFilter`.

## IV. Summary of Phase 1 Deliverables

*   A `Job` dataclass representing a background task.
*   A set of classes for core job states (`Enqueued`, `Processing`, `Succeeded`, `Failed`).
*   A thread-safe `MemoryStorage` class that can queue and dequeue jobs.
*   A `BackgroundJobClient` that can create fire-and-forget jobs.
*   A single-threaded `Worker` that polls the `MemoryStorage`, processes one job at a time, and correctly handles success and failure by updating job states.
*   The foundational `JobFilter` and `RetryFilter` classes, laying the groundwork for more advanced features.
*   A clear project structure that is ready for expansion in subsequent phases.