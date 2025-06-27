# pytaskflow/filters/base.py
from abc import ABC, abstractmethod


class JobFilter(ABC):
    def on_state_election(self, elect_state_context):
        pass  # Default implementation does nothing
