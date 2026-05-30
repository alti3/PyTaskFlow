# pytaskflow/serialization/base.py
from abc import ABC, abstractmethod
from typing import Callable, Tuple, Dict, Any

from pytaskflow.common.job import Job


class BaseSerializer(ABC):
    @abstractmethod
    def serialize_job(self, job: Job) -> str: ...

    @abstractmethod
    def deserialize_job(self, data: str) -> Job: ...

    @abstractmethod
    def serialize_args(
        self, target_func: Callable, *args: Any, **kwargs: Any
    ) -> Tuple[str, str]: ...

    @abstractmethod
    def deserialize_args(
        self, args_str: str, kwargs_str: str
    ) -> Tuple[Tuple, Dict]: ...

    @abstractmethod
    def serialize_state_data(self, data: Dict[str, Any]) -> str: ...

    @abstractmethod
    def deserialize_state_data(self, data_str: str) -> Dict[str, Any]: ...
