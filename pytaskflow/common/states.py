# pytaskflow/common/states.py
from abc import ABC, abstractmethod
from typing import Dict, Any, final, Optional
from datetime import datetime, UTC

class BaseState(ABC):
    """
    Abstract base class for all job states.
    Inspired by Hangfire.States.IState.
    """
    NAME: str = "State"

    def __init__(self, reason: Optional[str] = None):
        self.reason = reason

    @final
    @property
    def name(self) -> str:
        return self.NAME

    @abstractmethod
    def serialize_data(self) -> Dict[str, Any]:
        """Serializes state-specific data into a dictionary."""
        raise NotImplementedError

class EnqueuedState(BaseState):
    NAME = "Enqueued"
    
    def __init__(self, queue: str = "default", reason: Optional[str] = None):
        super().__init__(reason)
        self.queue = queue

    def serialize_data(self) -> Dict[str, Any]:
        data = {"queue": self.queue}
        if self.reason:
            data["reason"] = self.reason
        return data

class ScheduledState(BaseState):
    NAME = "Scheduled"
    
    def __init__(self, enqueue_at: datetime, scheduled_at: datetime, reason: Optional[str] = None):
        super().__init__(reason)
        self.enqueue_at = enqueue_at
        self.scheduled_at = scheduled_at

    def serialize_data(self) -> Dict[str, Any]:
        return {
            "enqueue_at": self.enqueue_at.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat(),
        }

class ProcessingState(BaseState):
    NAME = "Processing"

    def __init__(self, server_id: str, worker_id: str, reason: Optional[str] = None):
        super().__init__(reason)
        self.server_id = server_id
        self.worker_id = worker_id
        
    def serialize_data(self) -> Dict[str, Any]:
        return {"server_id": self.server_id, "worker_id": self.worker_id}

class SucceededState(BaseState):
    NAME = "Succeeded"
    
    def __init__(self, result: Any, reason: Optional[str] = None):
        super().__init__(reason)
        self.result = result # Note: result must be serializable
        
    def serialize_data(self) -> Dict[str, Any]:
        # The serializer will handle the result object
        return {"result": self.result}

class FailedState(BaseState):
    NAME = "Failed"

    def __init__(self, exception_type: str, exception_message: str, exception_details: str, reason: Optional[str] = None):
        super().__init__(reason)
        self.exception_type = exception_type
        self.exception_message = exception_message
        self.exception_details = exception_details # Full traceback

    def serialize_data(self) -> Dict[str, Any]:
        return {
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "exception_details": self.exception_details,
        }
