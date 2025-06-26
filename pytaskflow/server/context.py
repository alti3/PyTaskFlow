from pytaskflow.common.job import Job
from pytaskflow.common.states import BaseState


class ElectStateContext:
    def __init__(self, job: Job, candidate_state: BaseState):
        self.job = job
        self.candidate_state = candidate_state
