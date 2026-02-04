# Building a Storage Backend

This guide documents the expectations for implementing a new storage backend for PyTaskFlow.

## Contract: `JobStorage`

Every backend must implement the abstract methods in `pytaskflow/storage/base.py`. These methods are used by the client, worker, and dashboard. Any missing or partially implemented method will cause runtime errors.

Key responsibilities:

- Persist job records (`enqueue`, `schedule`, `get_job_data`)
- Move jobs through states (`set_job_state`, `acknowledge`)
- Provide queues for workers (`dequeue`)
- Expose dashboard queries (`get_jobs_by_state`, `get_state_job_count`, `get_job_history`, `get_servers`, `get_recurring_jobs`)
- Track servers/heartbeats (`server_heartbeat`, `remove_server`)

## Most Important Requirement: Atomic `dequeue`

The `dequeue` operation must be **atomic**. If two workers ask for a job at the same time, only one should receive it. This is critical to avoid duplicate processing and data loss.

Why it matters:

- If a worker crashes after marking a job as `Processing` but before completing it, the system needs to know that the job was in-flight.
- If two workers race on the same job and both process it, you can get duplicate side effects (emails sent twice, payments double-charged, etc.).

Recommended approach:

- Use a transactional "fetch and lock" strategy.
- In SQL databases, this is typically:
  - `SELECT ... FOR UPDATE SKIP LOCKED` (Postgres/MySQL 8+)
  - Or a single atomic `UPDATE ... WHERE status = 'enqueued' ... RETURNING` pattern.
- The dequeue should also update the job state to `Processing` in the same transaction.

If the backend cannot guarantee atomic dequeue, it is not safe for multi-worker production use.

## Processing Safety and Crash Recovery

At minimum, a backend should record:

- `fetched_at` or `processing_started_at`
- `worker_id` and `server_id`
- `processing` vs `enqueued` status in the queue

This enables future safety features such as:

- Re-queueing jobs stuck in `Processing` beyond a timeout
- Visibility into worker crashes
- Auditing job lifecycles

PyTaskFlow does not yet auto-requeue stuck jobs, but new backends should **store the data** needed to enable this safely later.

## State Transitions and History

Every call to `set_job_state` should:

- Update the job record
- Record a history entry (`get_job_history` is used in the dashboard)
- Keep state data serialized as JSON

If a backend drops history or fails to store state data, the dashboard will lose critical context.

## Queues and Acknowledgements

Recommended queue behavior:

- `enqueue` inserts a queue record with status `enqueued`
- `dequeue` moves it to `processing`
- `acknowledge` removes it from the processing list
- `set_job_state(..., EnqueuedState)` should move a job back to `enqueued`

This mirrors the behavior of the Redis backend and keeps job lifecycle consistent.

## Scheduled and Recurring Jobs

Backends should support:

- Scheduled jobs (`schedule`) with a durable `enqueue_at`
- Recurring jobs with a cron expression and `last_execution`

If a backend wants these to work under the worker schedulers, it should expose helper methods to move due scheduled/recurring jobs into the queue atomically.

## Dashboard Queries

The dashboard expects these queries to be fast and deterministic:

- List jobs by state with pagination
- Get counts per state
- Get job details and history
- List active servers
- List recurring jobs

Add indexes on `state_name`, `created_at`, and any queue lookup columns to keep the UI responsive.

## Summary Checklist

- Atomic dequeue with locking
- Durable processing metadata (worker, server, fetched_at)
- State transitions + history
- Scheduled and recurring storage
- Dashboard query support
- Reasonable indexes
