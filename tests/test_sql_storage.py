import pytest
from datetime import UTC, datetime, timedelta

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from pytaskflow.client import BackgroundJobClient
from pytaskflow.common.job import Job
from pytaskflow.common.states import EnqueuedState, FailedState, ScheduledState
from pytaskflow.storage.sql_storage import SqlStorage, QueueEntryModel


def _make_storage() -> SqlStorage:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    return SqlStorage(engine=engine, create_tables=True)


def test_sql_storage_enqueue_dequeue():
    storage = _make_storage()
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=EnqueuedState.NAME,
        state_data=EnqueuedState().serialize_data(),
    )
    storage.enqueue(job)

    dequeued = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert dequeued is not None
    assert dequeued.id == job.id
    assert dequeued.state_name == "Processing"

    stored = storage.get_job_data(job.id)
    assert stored is not None
    assert stored.state_name == "Processing"

    storage.acknowledge(job.id)


def test_sql_storage_requeue_from_failed():
    storage = _make_storage()
    client = BackgroundJobClient(storage)
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=EnqueuedState.NAME,
        state_data=EnqueuedState().serialize_data(),
    )
    storage.enqueue(job)
    storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )

    failed_state = FailedState(
        exception_type="ValueError",
        exception_message="boom",
        exception_details="trace",
    )
    assert storage.set_job_state(job.id, failed_state, expected_old_state="Processing")
    storage.acknowledge(job.id)

    assert client.requeue(job.id)
    requeued = storage.get_job_data(job.id)
    assert requeued is not None
    assert requeued.state_name == "Enqueued"

    dequeued = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert dequeued is not None
    assert dequeued.id == job.id


def test_sql_storage_scheduled_jobs_enqueue_due():
    storage = _make_storage()
    enqueue_at = datetime.now(UTC) - timedelta(seconds=1)
    scheduled_state = ScheduledState(enqueue_at, datetime.now(UTC))
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=scheduled_state.name,
        state_data=scheduled_state.serialize_data(),
    )
    storage.schedule(job, enqueue_at)

    moved = storage.enqueue_due_scheduled_jobs()
    assert job.id in moved

    dequeued = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert dequeued is not None
    assert dequeued.id == job.id


def test_sql_storage_recover_stuck_jobs():
    storage = _make_storage()
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=EnqueuedState.NAME,
        state_data=EnqueuedState().serialize_data(),
    )
    storage.enqueue(job)
    storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )

    stale_time = datetime.now(UTC) - timedelta(minutes=10)
    with storage._session_factory.begin() as session:
        entry = session.execute(
            sqlalchemy.select(QueueEntryModel).where(QueueEntryModel.job_id == job.id)
        ).scalar_one()
        entry.fetched_at = stale_time

    recovered = storage.recover_stuck_jobs(max_age_seconds=60)
    assert job.id in recovered

    dequeued = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert dequeued is not None
    assert dequeued.id == job.id
