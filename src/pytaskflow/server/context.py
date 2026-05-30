from typing import Any

from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState
from pytaskflow.storage.base import JobStorage


class ElectStateContext:
    def __init__(self, job: Job, candidate_state: BaseState):
        self.job = job
        self.candidate_state = candidate_state


class PerformingContext:
    def __init__(
        self,
        job: Job,
        storage: JobStorage,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        items: dict[str, Any],
    ):
        self.job = job
        self.storage = storage
        self.args = args
        self.kwargs = kwargs
        self.items = items
        self.canceled = False
        self.next_state: BaseState | None = None


class PerformedContext:
    def __init__(
        self,
        job: Job,
        storage: JobStorage,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        items: dict[str, Any],
        exception: BaseException | None = None,
    ):
        self.job = job
        self.storage = storage
        self.args = args
        self.kwargs = kwargs
        self.items = items
        self.exception = exception
