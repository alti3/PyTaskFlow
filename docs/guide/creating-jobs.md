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

## Continuations

Use `continue_with()` to enqueue follow-up work only after a parent job succeeds:

```python
parent_id = client.enqueue(import_customers, "customers.csv")
client.continue_with(parent_id, send_import_summary, "ops@example.com")
```

Continuation jobs start in the `Awaiting` state. When the parent reaches
`Succeeded`, PyTaskFlow moves each awaiting continuation to `Enqueued`.
