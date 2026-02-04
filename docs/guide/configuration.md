# Configuration

PyTaskFlow is configured by selecting a storage backend and using a client to enqueue jobs.

## Using Redis Storage

```python
from pytaskflow.client import BackgroundJobClient
from pytaskflow.storage.redis_storage import RedisStorage

storage = RedisStorage()
client = BackgroundJobClient(storage)
```

## Using SQLAlchemy Storage

```python
from pytaskflow.client import BackgroundJobClient
from pytaskflow.storage.sql_storage import SqlStorage

storage = SqlStorage(connection_url="sqlite:///pytaskflow.db")
client = BackgroundJobClient(storage)
```

## Global Configuration Helper

```python
import pytaskflow
from pytaskflow.storage.redis_storage import RedisStorage

pytaskflow.configure(storage=RedisStorage())
client = pytaskflow.get_client()
```
