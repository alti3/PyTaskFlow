# pytaskflow/storage/base.py
from abc import ABC, abstractmethod
from typing import Optional, List, Any
from datetime import datetime

from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState


class JobStorage(ABC):
    @abstractmethod
    def enqueue(self, job: Job) -> str: ...

    @abstractmethod
    def schedule(self, job: Job, enqueue_at: datetime) -> str: ...

    @abstractmethod
    def add_recurring_job(
        self, recurring_job_id: str, job_template: Job, cron_expression: str
    ): ...

    @abstractmethod
    def remove_recurring_job(self, recurring_job_id: str): ...

    @abstractmethod
    def trigger_recurring_job(self, recurring_job_id: str): ...

    @abstractmethod
    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]: ...

    @abstractmethod
    def acknowledge(self, job_id: str) -> None: ...

    @abstractmethod
    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool: ...

    @abstractmethod
    def get_job_data(self, job_id: str) -> Optional[Job]: ...

    @abstractmethod
    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None: ...
