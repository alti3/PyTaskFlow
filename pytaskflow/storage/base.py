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
