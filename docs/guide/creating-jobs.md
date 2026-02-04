# Creating Jobs

## Enqueue

```python
from pytaskflow.client import BackgroundJobClient

client = BackgroundJobClient(storage)
client.enqueue(send_email, "user@example.com")
```

## Schedule

```python
from datetime import datetime, UTC, timedelta

run_at = datetime.now(UTC) + timedelta(minutes=5)
client.schedule(send_email, run_at, "user@example.com")
```

## Recurring

```python
client.add_or_update_recurring(
    "daily-report",
    generate_report,
    cron_expression="0 0 * * *",
)
```
