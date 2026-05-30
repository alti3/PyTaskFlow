# pytaskflow/server/processor.py
import asyncio
import logging
import traceback
from typing import Sequence

from pytaskflow.common.job import Job
from pytaskflow.common.states import (
    AwaitingState,
    EnqueuedState,
    FailedState,
    ProcessingState,
    SucceededState,
)
from pytaskflow.common.exceptions import JobLoadError
from pytaskflow.execution.performer import perform_job, perform_job_async
from pytaskflow.filters.base import JobFilter
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.storage.base import JobStorage
from ..filters.builtin import RetryFilter
from .context import ElectStateContext, PerformedContext, PerformingContext

logger = logging.getLogger(__name__)


class JobProcessor:
    def __init__(
        self,
        job: Job,
        storage: JobStorage,
        serializer: BaseSerializer,
        filters: Sequence[JobFilter] | None = None,
    ):
        self.job = job
        self.storage = storage
        self.serializer = serializer
        self.filters = (
            list(filters) if filters is not None else [RetryFilter(attempts=3)]
        )

    def _trigger_continuations(self) -> None:
        continuation_ids = self.storage.get_continuations(self.job.id)
        if continuation_ids:
            logger.info(
                "Job %s succeeded. Triggering %d continuation(s).",
                self.job.id,
                len(continuation_ids),
            )
        for job_id in continuation_ids:
            job = self.storage.get_job_data(job_id)
            if job is None:
                continue
            self.storage.set_job_state(
                job_id,
                EnqueuedState(
                    queue=job.queue, reason=f"Continuation of job {self.job.id}"
                ),
                expected_old_state=AwaitingState.NAME,
            )

    def process(self):
        final_state = None
        args = ()
        kwargs = {}
        items = {}
        try:
            # 1. Deserialize args
            args, kwargs = self.serializer.deserialize_args(
                self.job.args, self.job.kwargs
            )

            performing_context = PerformingContext(
                self.job, self.storage, args, kwargs, items
            )
            for f in self.filters:
                f.on_performing(performing_context)
            if performing_context.canceled:
                final_state = performing_context.next_state
                if final_state is not None:
                    self.storage.set_job_state(
                        self.job.id,
                        final_state,
                        expected_old_state=ProcessingState.NAME,
                    )
                return

            # 2. Perform the job
            result = perform_job(
                self.job.target_module, self.job.target_function, args, kwargs
            )

            # 3. Set succeeded state
            succeeded_state = SucceededState(
                result=result, reason="Job performed successfully"
            )
            state_changed = self.storage.set_job_state(
                self.job.id, succeeded_state, expected_old_state=ProcessingState.NAME
            )
            final_state = succeeded_state
            if state_changed:
                self._trigger_continuations()

        except (Exception, JobLoadError) as e:
            # 4. Handle failure
            logger.error(f"Job {self.job.id} failed.", exc_info=True)
            exc_type = type(e).__name__
            exc_msg = str(e)
            exc_details = traceback.format_exc()

            failed_state = FailedState(
                exception_type=exc_type,
                exception_message=exc_msg,
                exception_details=exc_details,
            )

            # --- START FILTER INTEGRATION ---
            elect_state_context = ElectStateContext(
                job=self.job, candidate_state=failed_state
            )
            logger.debug(
                f"Job {self.job.id}: Before filters, retry_count={self.job.retry_count}, candidate_state={elect_state_context.candidate_state.name}"
            )

            for f in self.filters:
                f.on_state_election(elect_state_context)

            final_state = elect_state_context.candidate_state
            logger.debug(
                f"Job {self.job.id}: After filters, retry_count={self.job.retry_count}, final_state={final_state.name}"
            )

            # If the state changed to Enqueued (due to retry), update retry_count
            if isinstance(final_state, EnqueuedState):
                self.storage.update_job_field(
                    self.job.id, "retry_count", self.job.retry_count
                )

            # The set_job_state method will handle moving the job from processing back to a queue
            self.storage.set_job_state(
                self.job.id, final_state, expected_old_state=ProcessingState.NAME
            )
            # --- END FILTER INTEGRATION ---

        finally:
            performed_context = PerformedContext(
                self.job, self.storage, args, kwargs, items
            )
            for f in reversed(self.filters):
                f.on_performed(performed_context)

            # 5. Acknowledge completion. Acknowledging removes the job from the
            # processing list. This should only happen for jobs that have reached
            # a terminal state (Succeeded, Failed). If a job is re-enqueued for
            # retry, the set_job_state operation is responsible for moving it from
            # the processing list back to a queue.
            if final_state and not isinstance(final_state, EnqueuedState):
                self.storage.acknowledge(self.job.id)

    async def process_async(self):
        final_state = None
        args = ()
        kwargs = {}
        items = {}
        try:
            args, kwargs = self.serializer.deserialize_args(
                self.job.args, self.job.kwargs
            )

            performing_context = PerformingContext(
                self.job, self.storage, args, kwargs, items
            )
            for f in self.filters:
                f.on_performing(performing_context)
            if performing_context.canceled:
                final_state = performing_context.next_state
                if final_state is not None:
                    await asyncio.to_thread(
                        self.storage.set_job_state,
                        self.job.id,
                        final_state,
                        ProcessingState.NAME,
                    )
                return

            result = await perform_job_async(
                self.job.target_module, self.job.target_function, args, kwargs
            )

            succeeded_state = SucceededState(
                result=result, reason="Job performed successfully"
            )
            state_changed = await asyncio.to_thread(
                self.storage.set_job_state,
                self.job.id,
                succeeded_state,
                ProcessingState.NAME,
            )
            final_state = succeeded_state
            if state_changed:
                await asyncio.to_thread(self._trigger_continuations)

        except (Exception, JobLoadError) as e:
            logger.error(f"Job {self.job.id} failed.", exc_info=True)
            exc_type = type(e).__name__
            exc_msg = str(e)
            exc_details = traceback.format_exc()

            failed_state = FailedState(
                exception_type=exc_type,
                exception_message=exc_msg,
                exception_details=exc_details,
            )

            elect_state_context = ElectStateContext(
                job=self.job, candidate_state=failed_state
            )
            logger.debug(
                f"Job {self.job.id}: Before filters, retry_count={self.job.retry_count}, candidate_state={elect_state_context.candidate_state.name}"
            )

            for f in self.filters:
                f.on_state_election(elect_state_context)

            final_state = elect_state_context.candidate_state
            logger.debug(
                f"Job {self.job.id}: After filters, retry_count={self.job.retry_count}, final_state={final_state.name}"
            )

            if isinstance(final_state, EnqueuedState):
                await asyncio.to_thread(
                    self.storage.update_job_field,
                    self.job.id,
                    "retry_count",
                    self.job.retry_count,
                )

            await asyncio.to_thread(
                self.storage.set_job_state,
                self.job.id,
                final_state,
                ProcessingState.NAME,
            )

        finally:
            performed_context = PerformedContext(
                self.job, self.storage, args, kwargs, items
            )
            for f in reversed(self.filters):
                f.on_performed(performed_context)

            if final_state and not isinstance(final_state, EnqueuedState):
                await asyncio.to_thread(self.storage.acknowledge, self.job.id)
