# pytaskflow/storage/memory_storage.py
import logging
import time
from collections import deque
from threading import RLock, Condition
from typing import Optional, List, Dict, Any
from datetime import datetime, UTC

from pytaskflow.storage.base import JobStorage
from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState, ProcessingState, EnqueuedState

logger = logging.getLogger(__name__)


class MemoryStorage(JobStorage):
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._queues: Dict[str, deque[str]] = {}
        self._processing: Dict[str, Job] = {}  # Jobs currently being processed
        self._scheduled: Dict[str, datetime] = {}  # job_id -> enqueue_at
        self._recurring_jobs: Dict[str, dict] = {}  # recurring_job_id -> job data
        self._lock = RLock()
        self._condition = Condition(self._lock)

    def enqueue(self, job: Job) -> str:
        with self._lock:
            self._jobs[job.id] = job
            if job.queue not in self._queues:
                self._queues[job.queue] = deque()
            self._queues[job.queue].append(job.id)
            self._condition.notify()  # Notify any waiting worker
        return job.id

    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        with self._lock:
            self._jobs[job.id] = job
            self._scheduled[job.id] = enqueue_at
        return job.id

    def add_recurring_job(
        self, recurring_job_id: str, job_template: Job, cron_expression: str
    ):
        with self._lock:
            data = {
                "job": job_template.__dict__,
                "cron": cron_expression,
                "last_execution": None,
            }
            self._recurring_jobs[recurring_job_id] = data

    def _get_current_time(self) -> datetime:
        return datetime.now(UTC)

    def remove_recurring_job(self, recurring_job_id: str):
        with self._lock:
            if recurring_job_id in self._recurring_jobs:
                del self._recurring_jobs[recurring_job_id]

    def trigger_recurring_job(self, recurring_job_id: str):
        with self._lock:
            if recurring_job_id not in self._recurring_jobs:
                return

            data = self._recurring_jobs[recurring_job_id]
            job_dict = data["job"]

            # Create a new job instance from the template and enqueue it immediately
            job_instance = Job(
                target_module=job_dict["target_module"],
                target_function=job_dict["target_function"],
                args=job_dict["args"],
                kwargs=job_dict["kwargs"],
                state_name=EnqueuedState.NAME,
                state_data=EnqueuedState().serialize_data(),
                queue=job_dict.get("queue", "default"),
            )
            self.enqueue(job_instance)

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        with self._lock:
            start_time = time.monotonic()
            while True:  # This loop will now work as intended
                for queue_name in queues:
                    if queue_name in self._queues and self._queues[queue_name]:
                        job_id = self._queues[queue_name].popleft()
                        job = self._jobs[job_id]

                        self.set_job_state(
                            job.id, ProcessingState("server-mvp", "worker-1")
                        )
                        self._processing[job.id] = job
                        return job

                remaining_timeout = timeout_seconds - (time.monotonic() - start_time)
                if remaining_timeout <= 0:
                    return None

                # Wait for a job to be enqueued or for the timeout to expire
                self._condition.wait(timeout=remaining_timeout)

    def acknowledge(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._processing:
                del self._processing[job_id]

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        with self._lock:
            if job_id not in self._jobs:
                return False

            job = self._jobs[job_id]
            if expected_old_state and job.state_name != expected_old_state:
                return False

            job.state_name = state.name
            job.state_data = state.serialize_data()

            # If re-enqueued (for a retry), move it from processing back to a queue
            if isinstance(state, EnqueuedState):
                if job_id in self._processing:
                    del self._processing[job_id]
                
                if job.queue not in self._queues:
                    self._queues[job.queue] = deque()
                self._queues[job.queue].append(job.id)
                self._condition.notify()

            return True

    def get_job_data(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                setattr(job, field_name, value)