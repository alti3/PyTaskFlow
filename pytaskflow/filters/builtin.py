# pytaskflow/filters/builtin.py
from pytaskflow.filters.base import JobFilter
from pytaskflow.common.states import EnqueuedState, FailedState
import logging
from pytaskflow.server.context import ElectStateContext

logger = logging.getLogger(__name__)


class RetryFilter(JobFilter):
    def __init__(self, attempts: int = 3):
        self.attempts = attempts

    def on_state_election(self, elect_state_context: ElectStateContext):
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
