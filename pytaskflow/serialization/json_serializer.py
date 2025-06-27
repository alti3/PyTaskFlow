# pytaskflow/serialization/json_serializer.py
import json
from typing import Callable, Tuple, Dict, Any

from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.common.job import Job


class JsonSerializer(BaseSerializer):
    def serialize_job(self, job: Job) -> str:
        # For Phase 1, we can just serialize the dataclass. Later, this might get more complex.
        # This assumes state_data is already prepared for JSON.
        return json.dumps(job.__dict__, default=str)

    def deserialize_job(self, data: str) -> Job:
        job_dict = json.loads(data)
        return Job(**job_dict)

    def serialize_args(
        self, target_func: Callable, *args: Any, **kwargs: Any
    ) -> Tuple[str, str]:
        # This method is more for the client-side to prepare the job
        # For now, let's assume the client prepares serialized args directly.
        # This method will become more important with decorators.
        return json.dumps(args), json.dumps(kwargs)

    def deserialize_args(self, args_str: str, kwargs_str: str) -> Tuple[Tuple, Dict]:
        return tuple(json.loads(args_str)), json.loads(kwargs_str)

    def serialize_state_data(self, data: Dict[str, Any]) -> str:
        # Special handling for non-serializable objects like 'result' in SucceededState might be needed
        # For MVP, we'll assume basic types.
        return json.dumps(data)

    def deserialize_state_data(self, data_str: str) -> Dict[str, Any]:
        return json.loads(data_str)
