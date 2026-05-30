# pytaskflow/filters/builtin.py
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from pytaskflow.common.states import EnqueuedState, FailedState, ScheduledState
from pytaskflow.filters.base import JobFilter

if TYPE_CHECKING:
    from pytaskflow.server.context import (
        ElectStateContext,
        PerformedContext,
        PerformingContext,
    )

logger = logging.getLogger(__name__)


class RetryFilter(JobFilter):
    def __init__(self, attempts: int = 3):
        self.attempts = attempts

    def on_state_election(self, elect_state_context: "ElectStateContext"):
        job = elect_state_context.job
        candidate_state = elect_state_context.candidate_state

        if isinstance(candidate_state, FailedState):
            current_retry_count = job.retry_count
            logger.debug(
                f"RetryFilter: Job {job.id} failed. Current retry count: {current_retry_count}, Max attempts: {self.attempts}"
            )

            if current_retry_count < self.attempts:
                # Increment retry count and re-enqueue
                job.retry_count += 1
                logger.debug(
                    f"RetryFilter: Re-enqueuing job {job.id}. New retry count: {job.retry_count}"
                )

                # Change the candidate state to Enqueued
                new_reason = (
                    f"Retrying job... Attempt {job.retry_count} of {self.attempts}"
                )
                elect_state_context.candidate_state = EnqueuedState(
                    queue=job.queue, reason=new_reason
                )
            else:
                logger.debug(
                    f"RetryFilter: Job {job.id} retries exhausted. Moving to Failed state."
                )


class DisableConcurrentExecution(JobFilter):
    def __init__(
        self,
        resource_key: str,
        timeout_seconds: float = 300,
        retry_delay_seconds: float = 60,
    ):
        self.resource_key = resource_key
        self.timeout_seconds = timeout_seconds
        self.retry_delay_seconds = retry_delay_seconds

    def on_performing(self, performing_context: "PerformingContext") -> None:
        formatted_key = self.resource_key.format(
            *performing_context.args, **performing_context.kwargs
        )
        lock = performing_context.storage.acquire_distributed_lock(
            f"pytaskflow:lock:{formatted_key}", self.timeout_seconds
        )
        if lock is None:
            performing_context.canceled = True
            enqueue_at = datetime.now(timezone.utc) + timedelta(
                seconds=self.retry_delay_seconds
            )
            performing_context.next_state = ScheduledState(
                enqueue_at=enqueue_at,
                scheduled_at=datetime.now(timezone.utc),
                reason=f"Could not acquire lock on resource: {formatted_key}",
            )
            return

        performing_context.items["distributed_lock"] = lock

    def on_performed(self, performed_context: "PerformedContext") -> None:
        lock = performed_context.items.pop("distributed_lock", None)
        if lock is not None:
            release = getattr(lock, "release", None)
            if callable(release):
                release()
