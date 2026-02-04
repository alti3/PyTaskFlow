import pytest
import time
import json
from datetime import datetime, UTC, timedelta
from threading import Thread

from pytaskflow.common.job import Job
from pytaskflow.common.states import (
    EnqueuedState,
    ProcessingState,
    SucceededState,
    FailedState,
    ScheduledState,
)
from pytaskflow.common.exceptions import JobLoadError
from pytaskflow.serialization.json_serializer import JsonSerializer
from pytaskflow.storage.memory_storage import MemoryStorage
from pytaskflow.client import BackgroundJobClient
from pytaskflow.execution.performer import perform_job
from pytaskflow.server.processor import JobProcessor
from pytaskflow.server.worker import Worker
from pytaskflow.config import configure, get_storage


# --- Helper functions for testing jobs ---
def my_test_function(a, b):
    return a + b


def my_failing_function():
    raise ValueError("Test failure")


def my_side_effect_function(file_path, content):
    with open(file_path, "w") as f:
        f.write(content)


# --- Fixtures ---
@pytest.fixture
def json_serializer():
    return JsonSerializer()


@pytest.fixture
def memory_storage():
    storage = MemoryStorage()
    configure(storage)  # Configure global storage for client
    return storage


@pytest.fixture
def client(memory_storage, json_serializer):
    return BackgroundJobClient(memory_storage, json_serializer)


# --- Phase 1 Tests ---


# Test Job Dataclass
def test_job_creation():
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    assert job.id is not None
    assert isinstance(job.created_at, datetime)
    assert job.created_at.date() == datetime.now(UTC).date()
    assert job.target_module == "tests.test_tasks"
    assert job.target_function == "success_task"
    assert job.state_name == EnqueuedState.NAME
    assert job.queue == "default"
    assert job.retry_count == 0


# Test State Definitions
def test_enqueued_state():
    state = EnqueuedState(queue="high_priority", reason="New job")
    assert state.name == "Enqueued"
    assert state.queue == "high_priority"
    assert state.reason == "New job"
    assert state.serialize_data() == {"queue": "high_priority", "reason": "New job"}


def test_processing_state():
    state = ProcessingState("server1", "worker123")
    assert state.name == "Processing"
    assert state.server_id == "server1"
    assert state.worker_id == "worker123"
    assert state.serialize_data() == {"server_id": "server1", "worker_id": "worker123"}


def test_succeeded_state():
    state = SucceededState(result=42, reason="Completed successfully")
    assert state.name == "Succeeded"
    assert state.result == 42
    assert state.reason == "Completed successfully"
    assert state.serialize_data() == {"result": 42}


def test_failed_state():
    state = FailedState(
        "ValueError",
        "Something went wrong",
        "Traceback details",
        reason="Failed to execute",
    )
    assert state.name == "Failed"
    assert state.exception_type == "ValueError"
    assert state.exception_message == "Something went wrong"
    assert state.exception_details == "Traceback details"
    assert state.reason == "Failed to execute"
    assert state.serialize_data() == {
        "exception_type": "ValueError",
        "exception_message": "Something went wrong",
        "exception_details": "Traceback details",
    }


def test_scheduled_state():
    now = datetime.now(UTC)
    later = now + timedelta(minutes=5)
    state = ScheduledState(
        enqueue_at=later, scheduled_at=now, reason="Delayed execution"
    )
    assert state.name == "Scheduled"
    assert state.enqueue_at == later
    assert state.scheduled_at == now
    assert state.serialize_data() == {
        "enqueue_at": later.isoformat(),
        "scheduled_at": now.isoformat(),
    }


# Test JsonSerializer
def test_json_serializer_serialize_deserialize_args(json_serializer):
    args_str, kwargs_str = json_serializer.serialize_args(
        my_test_function, 1, "hello", kwarg1=True
    )
    assert json.loads(args_str) == [1, "hello"]
    assert json.loads(kwargs_str) == {"kwarg1": True}

    deserialized_args, deserialized_kwargs = json_serializer.deserialize_args(
        args_str, kwargs_str
    )
    assert deserialized_args == (1, "hello")
    assert deserialized_kwargs == {"kwarg1": True}


def test_json_serializer_serialize_deserialize_job(json_serializer):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    serialized_job = json_serializer.serialize_job(job)
    deserialized_job = json_serializer.deserialize_job(serialized_job)

    assert deserialized_job.id == job.id
    assert deserialized_job.target_module == job.target_module
    assert deserialized_job.target_function == job.target_function
    assert deserialized_job.state_name == job.state_name
    assert json.loads(deserialized_job.args) == json.loads(job.args)
    assert json.loads(deserialized_job.kwargs) == json.loads(job.kwargs)


def test_json_serializer_serialize_deserialize_state_data(json_serializer):
    data = {"key": "value", "number": 123}
    serialized_data = json_serializer.serialize_state_data(data)
    assert json.loads(serialized_data) == data
    assert json_serializer.deserialize_state_data(serialized_data) == data


# Test MemoryStorage
def test_memory_storage_enqueue_dequeue(memory_storage):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    job_id = memory_storage.enqueue(job)
    assert job_id == job.id

    dequeued_job = memory_storage.dequeue(
        ["default"], timeout_seconds=1, server_id="server-test", worker_id="worker-test"
    )
    assert dequeued_job.id == job.id
    assert (
        dequeued_job.state_name == ProcessingState.NAME
    )  # State should change to Processing

    # Ensure it's removed from the queue
    assert (
        memory_storage.dequeue(
            ["default"],
            timeout_seconds=0.1,
            server_id="server-test",
            worker_id="worker-test",
        )
        is None
    )


def test_memory_storage_set_job_state(memory_storage):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    memory_storage.enqueue(job)

    completed_state = SucceededState(result=42)
    success = memory_storage.set_job_state(
        job.id, completed_state, expected_old_state=EnqueuedState.NAME
    )
    assert success
    retrieved_job = memory_storage.get_job_data(job.id)
    assert retrieved_job.state_name == SucceededState.NAME
    assert retrieved_job.state_data == completed_state.serialize_data()

    # Test failed state change due to expected_old_state mismatch
    failed_state = FailedState("Error", "msg", "details")
    success = memory_storage.set_job_state(
        job.id, failed_state, expected_old_state=EnqueuedState.NAME
    )
    assert not success  # Should fail because current state is Completed


def test_memory_storage_acknowledge(memory_storage):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    memory_storage.enqueue(job)
    dequeued_job = memory_storage.dequeue(
        ["default"], timeout_seconds=1, server_id="server-test", worker_id="worker-test"
    )
    assert dequeued_job.id == job.id

    memory_storage.acknowledge(job.id)
    # Acknowledged jobs are removed from _processing, but remain in _jobs
    assert job.id not in memory_storage._processing
    assert memory_storage.get_job_data(job.id) is not None


def test_memory_storage_get_job_data(memory_storage):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME,
    )
    memory_storage.enqueue(job)
    retrieved_job = memory_storage.get_job_data(job.id)
    assert retrieved_job.id == job.id
    assert retrieved_job.state_name == EnqueuedState.NAME

    assert memory_storage.get_job_data("non_existent_id") is None


# Test BackgroundJobClient
def test_client_enqueue(client, memory_storage):
    job_id = client.enqueue(my_test_function, 10, 20)
    assert job_id is not None
    job = memory_storage.get_job_data(job_id)
    assert job.target_module == my_test_function.__module__
    assert job.target_function == "my_test_function"
    assert job.state_name == EnqueuedState.NAME
    assert json.loads(job.args) == [10, 20]
    assert json.loads(job.kwargs) == {}


def test_client_enqueue_with_kwargs(client, memory_storage):
    job_id = client.enqueue(my_test_function, a=5, b=7)
    job = memory_storage.get_job_data(job_id)
    assert json.loads(job.args) == []
    assert json.loads(job.kwargs) == {"a": 5, "b": 7}


# Test Job Performer
def test_perform_job_success():
    result = perform_job("tests.test_tasks", "success_task", (5, 3), {})
    assert result == 8


def test_perform_job_failure_module_not_found():
    with pytest.raises(
        JobLoadError, match="Could not load job target: non_existent_module.some_func"
    ):
        perform_job("non_existent_module", "some_func", (), {})


def test_perform_job_failure_function_not_found():
    with pytest.raises(
        JobLoadError,
        match="Could not load job target: tests.test_tasks.non_existent_func",
    ):
        perform_job("tests.test_tasks", "non_existent_func", (), {})


# Test JobProcessor
def test_job_processor_success(memory_storage, json_serializer):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([10, 5]),
        kwargs=json.dumps({}),
        state_name=ProcessingState.NAME,  # Assume it's already dequeued and in processing
    )
    memory_storage.enqueue(job)  # Enqueue to make it available for get_job_data
    memory_storage.set_job_state(
        job.id, ProcessingState("s1", "w1")
    )  # Manually set to processing

    processor = JobProcessor(job, memory_storage, json_serializer)
    processor.process()

    updated_job = memory_storage.get_job_data(job.id)
    assert updated_job.state_name == SucceededState.NAME
    assert updated_job.state_data["result"] == 15
    assert job.id not in memory_storage._processing  # Should be acknowledged


def test_job_processor_failure(memory_storage, json_serializer):
    job = Job(
        target_module="tests.test_tasks",
        target_function="failure_task",
        args=json.dumps([]),
        kwargs=json.dumps({}),
        state_name=ProcessingState.NAME,
    )
    memory_storage.enqueue(job)
    memory_storage.set_job_state(job.id, ProcessingState("s1", "w1"))

    processor = JobProcessor(job, memory_storage, json_serializer)
    processor.process()

    updated_job = memory_storage.get_job_data(job.id)
    # Due to RetryFilter, a failed job will be re-enqueued if retries are available
    assert updated_job.state_name == EnqueuedState.NAME
    assert updated_job.retry_count == 1
    assert "Retrying job" in updated_job.state_data["reason"]
    assert job.id not in memory_storage._processing  # Should be acknowledged


# Test RetryFilter
def test_retry_filter_requeues_failed_job(memory_storage, json_serializer):
    job = Job(
        target_module="tests.test_tasks",
        target_function="failure_task",
        args=json.dumps([]),
        kwargs=json.dumps({}),
        state_name=ProcessingState.NAME,
        retry_count=0,  # First attempt
    )
    memory_storage.enqueue(job)
    memory_storage.set_job_state(job.id, ProcessingState("s1", "w1"))

    processor = JobProcessor(job, memory_storage, json_serializer)
    # Processor has RetryFilter(attempts=3) by default
    processor.process()

    updated_job = memory_storage.get_job_data(job.id)
    assert updated_job.state_name == EnqueuedState.NAME  # Should be re-enqueued
    assert updated_job.retry_count == 1
    assert "Retrying job" in updated_job.state_data["reason"]

    # Simulate second failure
    # Need to manually set state back to Processing for the processor to pick it up again
    memory_storage.set_job_state(
        job.id, ProcessingState("s1", "w1"), expected_old_state=EnqueuedState.NAME
    )
    processor.process()
    updated_job = memory_storage.get_job_data(job.id)
    assert updated_job.state_name == EnqueuedState.NAME
    assert updated_job.retry_count == 2

    # Simulate third failure (last retry)
    memory_storage.set_job_state(
        job.id, ProcessingState("s1", "w1"), expected_old_state=EnqueuedState.NAME
    )
    processor.process()
    updated_job = memory_storage.get_job_data(job.id)
    assert updated_job.state_name == EnqueuedState.NAME
    assert updated_job.retry_count == 3

    # Simulate fourth failure (exceeds attempts)
    memory_storage.set_job_state(
        job.id, ProcessingState("s1", "w1"), expected_old_state=EnqueuedState.NAME
    )
    processor = JobProcessor(
        job, memory_storage, json_serializer
    )  # Re-initialize processor to reset filters
    processor.process()
    updated_job = memory_storage.get_job_data(job.id)
    assert updated_job.state_name == FailedState.NAME  # Should finally fail
    assert (
        updated_job.retry_count == 3
    )  # Retry count doesn't increment past max attempts


# Test Worker (basic functionality)
def test_worker_processes_single_job(memory_storage, json_serializer, client):
    job_id = client.enqueue(my_test_function, 100, 200)

    worker = Worker(memory_storage, json_serializer, queues=["default"])

    # Run worker in a separate thread for a short period
    worker_thread = Thread(target=worker.run)
    worker_thread.start()
    time.sleep(2)  # Give worker time to process
    worker._shutdown_requested = True  # Signal shutdown
    worker_thread.join()

    updated_job = memory_storage.get_job_data(job_id)
    assert updated_job.state_name == SucceededState.NAME
    assert updated_job.state_data["result"] == 300


# Test global config
def test_global_config(memory_storage):
    # Already configured by memory_storage fixture
    retrieved_storage = get_storage()
    assert retrieved_storage is memory_storage

    # Test error when not configured
    configure(None)  # Clear config
    with pytest.raises(RuntimeError, match="PyTaskFlow has not been configured"):
        get_storage()
    configure(memory_storage)  # Restore for other tests
