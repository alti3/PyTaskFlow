# Litestar Integration

Install the dashboard extra (includes Litestar):

```bash
uv add "pytaskflow[dashboard]"
```

## Configure Dependency Injection

```python
from litestar import Litestar
from litestar.di import Provide
from pytaskflow.integrations.litestar import configure_pytaskflow, get_pytaskflow_client
from pytaskflow.storage.redis_storage import RedisStorage

app = Litestar(
    route_handlers=[],
    dependencies={"client": Provide(get_pytaskflow_client, sync_to_thread=False)},
)

configure_pytaskflow(app, RedisStorage())
```
