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
    def dequeue(
        self, queues: List[str], timeout_seconds: int, server_id: str, worker_id: str
    ) -> Optional[Job]: ...

    @abstractmethod
    def acknowledge(self, job_id: str) -> None: ...

    @abstractmethod
    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool: ...

    @abstractmethod
    def get_job_data(self, job_id: str) -> Optional[Job]: ...

    @abstractmethod
    def get_jobs_by_state(
        self, state_name: str, start: int, count: int
    ) -> List[Job]: ...

    @abstractmethod
    def get_job_ids_by_state(
        self, state_name: str, start: int, count: int
    ) -> List[str]: ...

    @abstractmethod
    def get_state_job_count(self, state_name: str) -> int: ...

    @abstractmethod
    def get_all_servers(self) -> List[dict]: ...

    @abstractmethod
    def server_heartbeat(
        self, server_id: str, worker_count: int, queues: List[str]
    ): ...

    @abstractmethod
    def remove_server(self, server_id: str): ...

    @abstractmethod
    def get_servers(self) -> List[dict]: ...

    @abstractmethod
    def get_recurring_jobs(self, start: int, count: int) -> List[dict]: ...

    @abstractmethod
    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None: ...

    @abstractmethod
    def get_job_history(self, job_id: str) -> List[dict]: ...

    @abstractmethod
    def get_statistics(self) -> dict: ...
