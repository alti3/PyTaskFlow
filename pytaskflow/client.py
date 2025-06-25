# pytaskflow/client.py
from typing import Callable, Any, Optional

from pytaskflow.common.job import Job
from pytaskflow.common.states import EnqueuedState
from pytaskflow.storage.base import JobStorage
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.serialization.json_serializer import JsonSerializer

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
