from datetime import datetime, timedelta, timezone

import pytest

from pytaskflow.client import BackgroundJobClient
from pytaskflow.common.job import Job
from pytaskflow.common.states import (
    AwaitingState,
    EnqueuedState,
    ScheduledState,
    SucceededState,
)
from pytaskflow.filters.builtin import DisableConcurrentExecution
from pytaskflow.serialization.json_serializer import JsonSerializer
from pytaskflow.server.processor import JobProcessor
from pytaskflow.storage.base import StateHandler
from pytaskflow.storage.memory_storage import MemoryStorage
from tests.test_tasks import success_task

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from pytaskflow.storage.sql_storage import SqlStorage  # noqa: E402


def _make_sql_storage() -> SqlStorage:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    return SqlStorage(engine=engine, create_tables=True)


@pytest.mark.parametrize("storage_factory", [MemoryStorage, _make_sql_storage])
def test_continue_with_enqueues_child_after_parent_success(storage_factory):
    storage = storage_factory()
    serializer = JsonSerializer()
    client = BackgroundJobClient(storage, serializer)

    parent_id = client.enqueue(success_task, 1, 2)
    child_id = client.continue_with(parent_id, success_task, 3, 4)

    child = storage.get_job_data(child_id)
    assert child is not None
    assert child.state_name == AwaitingState.NAME

    parent = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert parent is not None
    JobProcessor(parent, storage, serializer).process()

    child = storage.get_job_data(child_id)
    assert child is not None
    assert child.state_name == EnqueuedState.NAME

    dequeued_child = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-2"
    )
    assert dequeued_child is not None
    assert dequeued_child.id == child_id


def test_continue_with_parent_already_succeeded_enqueues_immediately():
    storage = MemoryStorage()
    serializer = JsonSerializer()
    client = BackgroundJobClient(storage, serializer)

    parent = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=SucceededState.NAME,
        state_data=SucceededState(result=3).serialize_data(),
    )
    storage.enqueue(parent)
    storage.set_job_state(parent.id, SucceededState(result=3))

    child_id = client.continue_with(parent.id, success_task, 3, 4)

    child = storage.get_job_data(child_id)
    assert child is not None
    assert child.state_name == EnqueuedState.NAME


def test_disable_concurrent_execution_reschedules_when_lock_is_held():
    storage = MemoryStorage()
    serializer = JsonSerializer()
    lock = storage.acquire_distributed_lock("pytaskflow:lock:shared", 30)
    assert lock is not None

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

    processor = JobProcessor(
        dequeued,
        storage,
        serializer,
        filters=[DisableConcurrentExecution("shared", retry_delay_seconds=1)],
    )
    processor.process()

    updated = storage.get_job_data(job.id)
    assert updated is not None
    assert updated.state_name == ScheduledState.NAME
    assert updated.state_data["enqueue_at"] > datetime.now(timezone.utc).isoformat()
    lock.release()


def test_disable_concurrent_execution_releases_lock_after_success():
    storage = MemoryStorage()
    serializer = JsonSerializer()
    client = BackgroundJobClient(storage, serializer)
    job_id = client.enqueue(success_task, 1, 2)
    job = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert job is not None

    processor = JobProcessor(
        job,
        storage,
        serializer,
        filters=[DisableConcurrentExecution("job:{0}", timeout_seconds=30)],
    )
    processor.process()

    updated = storage.get_job_data(job_id)
    assert updated is not None
    assert updated.state_name == SucceededState.NAME
    lock = storage.acquire_distributed_lock("pytaskflow:lock:job:1", 30)
    assert lock is not None
    lock.release()


def test_state_handlers_run_around_state_transition():
    events: list[tuple[str, str, str]] = []

    class RecordingHandler(StateHandler):
        def on_unapply(self, job, old_state, new_state):
            events.append(("unapply", old_state, new_state.name))

        def on_apply(self, job, old_state, new_state):
            events.append(("apply", old_state, new_state.name))

    class HandlerStorage(MemoryStorage):
        def get_state_handlers(self, state_name):
            return [RecordingHandler()]

    storage = HandlerStorage()
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=EnqueuedState.NAME,
        state_data=EnqueuedState().serialize_data(),
    )
    storage.enqueue(job)

    assert storage.set_job_state(job.id, SucceededState(result=3))
    assert events == [
        ("unapply", EnqueuedState.NAME, SucceededState.NAME),
        ("apply", EnqueuedState.NAME, SucceededState.NAME),
    ]


def test_memory_storage_moves_due_scheduled_jobs():
    storage = MemoryStorage()
    enqueue_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    state = ScheduledState(enqueue_at, datetime.now(timezone.utc))
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args="[1, 2]",
        kwargs="{}",
        state_name=state.name,
        state_data=state.serialize_data(),
    )
    storage.schedule(job, enqueue_at)

    assert storage.enqueue_due_scheduled_jobs() == [job.id]
    dequeued = storage.dequeue(
        ["default"], timeout_seconds=0, server_id="server-1", worker_id="worker-1"
    )
    assert dequeued is not None
    assert dequeued.id == job.id
