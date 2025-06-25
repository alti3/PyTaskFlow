# pytaskflow/storage/memory_storage.py
import time
from collections import deque
from threading import RLock, Condition
from typing import Optional, List, Dict

from pytaskflow.storage.base import JobStorage
from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState, ProcessingState

class MemoryStorage(JobStorage):
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._queues: Dict[str, deque[str]] = {}
        self._processing: Dict[str, Job] = {} # Jobs currently being processed
        self._lock = RLock()
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
            for queue_name in queues:
                if queue_name in self._queues and self._queues[queue_name]:
                    job_id = self._queues[queue_name].popleft()
                    job = self._jobs[job_id]
                    
                    # Atomically move to processing
                    self.set_job_state(job.id, ProcessingState("server-mvp", "worker-1"))
                    self._processing[job.id] = job
                    return job

            
            
            return None


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
