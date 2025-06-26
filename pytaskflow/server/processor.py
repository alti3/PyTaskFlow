# pytaskflow/server/processor.py
import traceback
import logging
from pytaskflow.common.job import Job
from pytaskflow.common.states import SucceededState, FailedState, ProcessingState, EnqueuedState
from pytaskflow.execution.performer import perform_job
from pytaskflow.storage.base import JobStorage
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.common.exceptions import JobLoadError
from ..filters.builtin import RetryFilter
from .context import ElectStateContext

logger = logging.getLogger(__name__)

class JobProcessor:
    def __init__(self, job: Job, storage: JobStorage, serializer: BaseSerializer):
        self.job = job
        self.storage = storage
        self.serializer = serializer
        self.filters = [RetryFilter(attempts=3)]

    def process(self):
        final_state = None  # Initialize final_state to ensure it's always bound
        try:
            # 1. Deserialize args
            args, kwargs = self.serializer.deserialize_args(self.job.args, self.job.kwargs)
            
            # 2. Perform the job
            result = perform_job(self.job.target_module, self.job.target_function, args, kwargs)
            
            # 3. Set succeeded state
            succeeded_state = SucceededState(result=result, reason="Job performed successfully")
            self.storage.set_job_state(self.job.id, succeeded_state, expected_old_state=ProcessingState.NAME)
            final_state = succeeded_state # Initialize final_state here for success path

        except (Exception, JobLoadError) as e:
            # 4. Handle failure
            logger.error(f"Job {self.job.id} failed.", exc_info=True)
            exc_type = type(e).__name__
            exc_msg = str(e)
            exc_details = traceback.format_exc()
            
            failed_state = FailedState(
                exception_type=exc_type, 
                exception_message=exc_msg, 
                exception_details=exc_details
            )
            
            # --- START FILTER INTEGRATION ---
            elect_state_context = ElectStateContext(job=self.job, candidate_state=failed_state)
            logger.debug(f"Job {self.job.id}: Before filters, retry_count={self.job.retry_count}, candidate_state={elect_state_context.candidate_state.name}")
            
            for f in self.filters:
                f.on_state_election(elect_state_context)
            
            final_state = elect_state_context.candidate_state
            logger.debug(f"Job {self.job.id}: After filters, retry_count={self.job.retry_count}, final_state={final_state.name}")

            # If the state changed to Enqueued (due to retry), update retry_count
            if isinstance(final_state, EnqueuedState):
                self.storage.update_job_field(self.job.id, "retry_count", self.job.retry_count)
                self.storage.set_job_state(self.job.id, final_state) # No expected_old_state for re-enqueue
            else:
                self.storage.set_job_state(self.job.id, final_state, expected_old_state=ProcessingState.NAME)

            # --- END FILTER INTEGRATION ---
        
        finally:
            # 5. Acknowledge completion only if job is truly finished
            if isinstance(final_state, SucceededState) or (isinstance(final_state, FailedState) and self.job.retry_count >= self.filters[0].attempts):
                self.storage.acknowledge(self.job.id)
