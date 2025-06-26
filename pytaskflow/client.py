# pytaskflow/client.py
from typing import Callable, Any, Optional
from datetime import datetime, UTC

from pytaskflow.common.job import Job
from pytaskflow.common.states import EnqueuedState, ScheduledState
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

    def add_or_update_recurring(self, recurring_job_id: str, target_func: Callable, cron_expression: str, *args, **kwargs):
        # Creates a job template (without an ID) to be stored.
        module_name = target_func.__module__
        func_name = target_func.__name__
        serialized_args = self.serializer.serialize_args(target_func, *args, **kwargs)
        job_template = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args[0],
            kwargs=serialized_args[1],
            state_name="", # State is not relevant for the template
        )
        self.storage.add_recurring_job(recurring_job_id, job_template, cron_expression)

    def remove_recurring_if_exists(self, recurring_job_id: str):
        self.storage.remove_recurring_job(recurring_job_id)

    def trigger(self, recurring_job_id: str):
        self.storage.trigger_recurring_job(recurring_job_id)
