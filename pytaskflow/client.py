# pytaskflow/client.py
from typing import Callable, Any, Optional, List, Dict
from datetime import datetime, UTC

from .common.job import Job
from .common.states import EnqueuedState, ScheduledState
from .storage.base import JobStorage
from .storage.redis_storage import RedisStorage
from .serialization.base import BaseSerializer
from .serialization.json_serializer import JsonSerializer


class Client:
    """
    A client for interacting with PyTaskFlow, enabling job enqueuing, scheduling,
    and querying for the dashboard.
    """
    def __init__(
        self, storage: Optional[JobStorage] = None, serializer: Optional[BaseSerializer] = None
    ):
        self.storage = storage or RedisStorage()
        self.serializer = serializer or JsonSerializer()

    def enqueue(self, target_func: Callable, *args: Any, **kwargs: Any) -> str:
        """Creates a fire-and-forget job."""
        module_name, func_name = target_func.__module__, target_func.__name__
        serialized_args = self.serializer.serialize_args(target_func, *args, **kwargs)
        job = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args[0],
            kwargs=serialized_args[1],
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data(),
        )
        return self.storage.enqueue(job)

    # ... (schedule, add_or_update_recurring, etc. remain the same)
    def schedule(
        self, target_func: Callable, enqueue_at: datetime, *args: Any, **kwargs: Any
    ) -> str:
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
            state_data=scheduled_state.serialize_data(),
        )

        job_id = self.storage.schedule(job, enqueue_at)
        return job_id

    def add_or_update_recurring(
        self,
        recurring_job_id: str,
        target_func: Callable,
        cron_expression: str,
        *args,
        **kwargs,
    ):
        # Creates a job template (without an ID) to be stored.
        module_name = target_func.__module__
        func_name = target_func.__name__
        serialized_args = self.serializer.serialize_args(target_func, *args, **kwargs)
        job_template = Job(
            target_module=module_name,
            target_function=func_name,
            args=serialized_args[0],
            kwargs=serialized_args[1],
            state_name="",  # State is not relevant for the template
        )
        self.storage.add_recurring_job(recurring_job_id, job_template, cron_expression)

    def remove_recurring_if_exists(self, recurring_job_id: str):
        self.storage.remove_recurring_job(recurring_job_id)

    def trigger(self, recurring_job_id: str):
        self.storage.trigger_recurring_job(recurring_job_id)

    # --- Dashboard Methods ---

    def get_jobs_by_state(
        self, state_name: str, page: int = 1, page_size: int = 20
    ) -> List[Job]:
        start = (page - 1) * page_size
        job_ids = self.storage.get_job_ids_by_state(state_name, start, page_size)
        if not job_ids:
            return []
        
        jobs = []
        for job_id in job_ids:
            job_data = self.storage.get_job_data(job_id)
            if job_data:
                jobs.append(job_data)
        return jobs

    def get_job_details(self, job_id: str) -> Optional[Job]:
        return self.storage.get_job_data(job_id)

    def get_state_counts(self) -> Dict[str, int]:
        from .common.states import ALL_STATES
        return {
            state: self.storage.get_state_job_count(state) for state in ALL_STATES
        }

    def get_servers(self) -> List[Dict]:
        return self.storage.get_all_servers()

    def get_recurring_jobs(self, page: int = 1, page_size: int = 20) -> List[Dict]:
        start = (page - 1) * page_size
        return self.storage.get_recurring_jobs(start, page_size)