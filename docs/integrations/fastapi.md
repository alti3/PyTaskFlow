# FastAPI Integration

Install the extra:

```bash
uv add "pytaskflow[fastapi]"
```

## Plugin Usage

```python
from fastapi import Depends, FastAPI
from pytaskflow.integrations.fastapi import add_pytaskflow_to_fastapi
from pytaskflow.storage.redis_storage import RedisStorage

app = FastAPI()
storage = RedisStorage()
ptf = add_pytaskflow_to_fastapi(app, storage)
ptf.include_dashboard(path="/pytaskflow")
ptf.run_worker_in_background(worker_count=2)

@app.post("/users")
def create_user(client = Depends(ptf.get_client)):
    client.enqueue(send_welcome_email)
```
