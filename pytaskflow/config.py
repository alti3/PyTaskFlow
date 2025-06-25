# pytaskflow/config.py
from typing import Optional
from pytaskflow.storage.base import JobStorage

class _GlobalConfig:
    def __init__(self):
        self.storage: Optional[JobStorage] = None

_GLOBAL_CONFIG = _GlobalConfig()

def configure(storage: JobStorage) -> None:
    _GLOBAL_CONFIG.storage = storage

def get_storage() -> JobStorage:
    if not _GLOBAL_CONFIG.storage:
        raise RuntimeError("PyTaskFlow has not been configured. Call pytaskflow.configure() first.")
    return _GLOBAL_CONFIG.storage
