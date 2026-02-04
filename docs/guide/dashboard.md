# Dashboard

The dashboard provides visibility into job states, servers, and recurring jobs. It also allows retrying or deleting failed jobs.

## Run the Dashboard

```bash
uv run run_dashboard.py --storage redis --redis-url redis://localhost:6379/0
```

The dashboard runs at `http://localhost:8000` by default.

## Actions

Failed jobs include actions to retry or delete them directly from the Jobs view.

## Recover Stuck Jobs

When viewing the `Processing` jobs list, a recovery form allows you to requeue jobs
that have been in `processing` longer than a given age threshold. This uses the
SQL storage backend's `recover_stuck_jobs()` utility.
