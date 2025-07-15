# pytaskflow/common/states.py

from datetime import datetime, UTC
from typing import Dict, Any


class BaseState:
    NAME = "base"

    def __init__(self, created_at: datetime = None):
        self.created_at = created_at or datetime.now(UTC)

    def serialize_data(self) -> Dict[str, Any]:
        return {"created_at": self.created_at.isoformat()}

    @classmethod
    def deserialize_data(cls, data: Dict[str, Any]) -> "BaseState":
        return cls(created_at=datetime.fromisoformat(data["created_at"]))


class EnqueuedState(BaseState):
    NAME = "enqueued"


class ScheduledState(BaseState):
    NAME = "scheduled"

    def __init__(self, enqueue_at: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enqueue_at = enqueue_at

    def serialize_data(self) -> Dict[str, Any]:
        data = super().serialize_data()
        data["enqueue_at"] = self.enqueue_at.isoformat()
        return data


class ProcessingState(BaseState):
    NAME = "processing"

    def __init__(self, server: str, worker_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.server = server
        self.worker_id = worker_id

    def serialize_data(self) -> Dict[str, Any]:
        data = super().serialize_data()
        data.update({"server": self.server, "worker_id": self.worker_id})
        return data


class CompletedState(BaseState):
    NAME = "completed"

    def __init__(self, result: Any, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = result

    def serialize_data(self) -> Dict[str, Any]:
        data = super().serialize_data()
        data["result"] = self.result
        return data


class FailedState(BaseState):
    NAME = "failed"

    def __init__(self, exception: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = exception

    def serialize_data(self) -> Dict[str, Any]:
        data = super().serialize_data()
        data["exception"] = self.exception
        return data


class CancelledState(BaseState):
    NAME = "cancelled"

ALL_STATES = [
    EnqueuedState.NAME,
    ScheduledState.NAME,
    ProcessingState.NAME,
    CompletedState.NAME,
    FailedState.NAME,
    CancelledState.NAME,
]