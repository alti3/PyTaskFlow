# pytaskflow/server/processor.py
import traceback
from pytaskflow.common.job import Job
from pytaskflow.common.states import SucceededState, FailedState, ProcessingState
from pytaskflow.execution.performer import perform_job
from pytaskflow.storage.base import JobStorage
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.common.exceptions import JobLoadError
from ..filters.builtin import RetryFilter
from .context import ElectStateContext

class JobProcessor:
    def __init__(self, job: Job, storage: JobStorage, serializer: BaseSerializer):
        self.job = job
        self.storage = storage
        self.serializer = serializer
        self.filters = [RetryFilter(attempts=3)]

    def process(self):
        try:
            # 1. Deserialize args
            args, kwargs = self.serializer.deserialize_args(self.job.args, self.job.kwargs)
            
            # 2. Perform the job
            result = perform_job(self.job.target_module, self.job.target_function, args, kwargs)
            
            # 3. Set succeeded state
            succeeded_state = SucceededState(result=result, reason="Job performed successfully")
            self.storage.set_job_state(self.job.id, succeeded_state, expected_old_state=ProcessingState.NAME)

        except (Exception, JobLoadError) as e:
            # 4. Handle failure
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
            
            for f in self.filters:
                f.on_state_election(elect_state_context)
            
            final_state = elect_state_context.candidate_state
            # --- END FILTER INTEGRATION ---

            self.storage.set_job_state(self.job.id, final_state, expected_old_state=ProcessingState.NAME)
        
        finally:
            # 5. Acknowledge completion
            self.storage.acknowledge(self.job.id)
