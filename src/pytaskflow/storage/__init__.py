from typing import Any

from .base import JobStorage
from .memory_storage import MemoryStorage
from .redis_storage import RedisStorage

SqlStorage: Any = None
try:  # Optional dependency
    from . import sql_storage as _sql_storage
except ImportError:  # pragma: no cover
    pass
else:
    SqlStorage = _sql_storage.SqlStorage

__all__ = ["JobStorage", "MemoryStorage", "RedisStorage", "SqlStorage"]
