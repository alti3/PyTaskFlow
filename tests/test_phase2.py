
import pytest
import time
import json
import redis
from datetime import datetime, UTC, timedelta
from threading import Thread

from pytaskflow.common.job import Job
from pytaskflow.common.states import EnqueuedState, ProcessingState, SucceededState, FailedState, ScheduledState
from pytaskflow.common.exceptions import JobLoadError
from pytaskflow.serialization.json_serializer import JsonSerializer
from pytaskflow.storage.redis_storage import RedisStorage
from pytaskflow.client import BackgroundJobClient
from pytaskflow.execution.performer import perform_job
from pytaskflow.server.processor import JobProcessor
from pytaskflow.server.worker import Worker
from pytaskflow.filters.builtin import RetryFilter
from pytaskflow.server.context import ElectStateContext
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
def redis_client():
    r = redis.Redis(host='localhost', port=6379, db=0)
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        pytest.skip("Redis server not running on localhost:6379")
    r.flushdb() # Clear database before each test
    return r

@pytest.fixture
def redis_storage(redis_client):
    storage = RedisStorage(connection_pool=redis_client.connection_pool)
    configure(storage) # Configure global storage for client
    return storage

@pytest.fixture
def json_serializer():
    return JsonSerializer()

@pytest.fixture
def client(redis_storage, json_serializer):
    return BackgroundJobClient(redis_storage, json_serializer)

# --- Phase 2 Tests ---

# Test RedisStorage
def test_redis_storage_enqueue_dequeue(redis_storage, redis_client):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME
    )
    job_id = redis_storage.enqueue(job)
    assert job_id == job.id

    # Check if job data is stored in Redis
    stored_job_data = redis_client.hgetall(f"pytaskflow:job:{job_id}")
    assert stored_job_data[b'target_function'].decode() == "success_task"
    assert redis_client.lrange(f"pytaskflow:queue:{job.queue}", 0, -1) == [job_id.encode()]

    dequeued_job = redis_storage.dequeue(["default"], timeout_seconds=1)
    assert dequeued_job.id == job.id
    assert dequeued_job.state_name == ProcessingState.NAME # State should change to Processing

    # Check if job is moved to processing list
    assert redis_client.lrange(f"pytaskflow:queue:processing", 0, -1) == [job_id.encode()]
    assert redis_client.lrange(f"pytaskflow:queue:{job.queue}", 0, -1) == []

    # Ensure it's removed from the queue
    assert redis_storage.dequeue(["default"], timeout_seconds=0.1) is None

def test_redis_storage_set_job_state(redis_storage, redis_client):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME
    )
    redis_storage.enqueue(job)

    succeeded_state = SucceededState(result=42)
    success = redis_storage.set_job_state(job.id, succeeded_state, expected_old_state=EnqueuedState.NAME)
    assert success
    retrieved_job = redis_storage.get_job_data(job.id)
    assert retrieved_job.state_name == SucceededState.NAME
    assert json.loads(retrieved_job.state_data["result"]) == succeeded_state.serialize_data()["result"]

    # Test failed state change due to expected_old_state mismatch
    failed_state = FailedState("Error", "msg", "details")
    success = redis_storage.set_job_state(job.id, failed_state, expected_old_state=EnqueuedState.NAME)
    assert not success # Should fail because current state is Succeeded

def test_redis_storage_acknowledge(redis_storage, redis_client):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME
    )
    redis_storage.enqueue(job)
    dequeued_job = redis_storage.dequeue(["default"], timeout_seconds=1)
    assert dequeued_job.id == job.id

    redis_storage.acknowledge(job.id)
    # Acknowledged jobs are removed from processing list
    assert redis_client.lrange(f"pytaskflow:queue:processing", 0, -1) == []
    assert redis_storage.get_job_data(job.id) is not None # Job data should still exist

def test_redis_storage_get_job_data(redis_storage):
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=EnqueuedState.NAME
    )
    redis_storage.enqueue(job)
    retrieved_job = redis_storage.get_job_data(job.id)
    assert retrieved_job.id == job.id
    assert retrieved_job.state_name == EnqueuedState.NAME

    assert redis_storage.get_job_data("non_existent_id") is None

def test_redis_storage_schedule(redis_storage, redis_client):
    now = datetime.now(UTC)
    enqueue_at = now + timedelta(seconds=10)
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=ScheduledState.NAME
    )
    job_id = redis_storage.schedule(job, enqueue_at)
    assert job_id == job.id

    # Check if job is in scheduled sorted set
    scheduled_jobs = redis_client.zrange("pytaskflow:scheduled", 0, -1, withscores=True)
    assert len(scheduled_jobs) == 1
    assert scheduled_jobs[0][0].decode() == job_id
    assert int(scheduled_jobs[0][1]) == int(enqueue_at.timestamp())

def test_redis_storage_add_remove_recurring_job(redis_storage, redis_client):
    job_template = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 2]),
        kwargs=json.dumps({}),
        state_name=""
    )
    cron_expression = "* * * * *"
    recurring_job_id = "my_daily_job"

    redis_storage.add_recurring_job(recurring_job_id, job_template, cron_expression)

    # Check if recurring job is stored
    stored_data = redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id)
    assert stored_data is not None
    data = json.loads(stored_data.decode())
    assert data["cron"] == cron_expression
    assert data["job"]["target_function"] == "success_task"
    assert redis_client.sismember("pytaskflow:recurring-jobs:ids", recurring_job_id)

    redis_storage.remove_recurring_job(recurring_job_id)
    assert redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id) is None
    assert not redis_client.sismember("pytaskflow:recurring-jobs:ids", recurring_job_id)

def test_redis_storage_trigger_recurring_job(redis_storage, redis_client):
    job_template = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([10, 20]),
        kwargs=json.dumps({}),
        state_name=""
    )
    cron_expression = "* * * * *"
    recurring_job_id = "my_triggerable_job"
    redis_storage.add_recurring_job(recurring_job_id, job_template, cron_expression)

    redis_storage.trigger_recurring_job(recurring_job_id)

    # Check if a new job is enqueued
    enqueued_job_ids = redis_client.lrange("pytaskflow:queue:default", 0, -1)
    assert len(enqueued_job_ids) == 1
    new_job_id = enqueued_job_ids[0].decode()
    new_job_data = redis_client.hgetall(f"pytaskflow:job:{new_job_id}")
    assert new_job_data[b'target_function'].decode() == "success_task"
    assert json.loads(new_job_data[b'args'].decode()) == [10, 20]

# Test BackgroundJobClient with RedisStorage
def test_client_schedule(client, redis_storage, redis_client):
    now = datetime.now(UTC)
    enqueue_at = now + timedelta(seconds=5)
    job_id = client.schedule(my_test_function, enqueue_at, 1, 2)
    assert job_id is not None

    job = redis_storage.get_job_data(job_id)
    assert job.state_name == ScheduledState.NAME
    assert int(redis_client.zscore("pytaskflow:scheduled", job_id)) == int(enqueue_at.timestamp())

def test_client_add_remove_trigger_recurring(client, redis_storage, redis_client):
    recurring_job_id = "test_recurring_job"
    client.add_or_update_recurring(recurring_job_id, my_test_function, "* * * * *", 100, 200)

    stored_data = redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id)
    assert stored_data is not None

    client.trigger(recurring_job_id)
    enqueued_job_ids = redis_client.lrange("pytaskflow:queue:default", 0, -1)
    assert len(enqueued_job_ids) == 1

    client.remove_recurring_if_exists(recurring_job_id)
    assert redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id) is None

# Test Worker with RedisStorage (Scheduler functionality)
def test_worker_processes_scheduled_job(redis_storage, json_serializer, redis_client):
    # Schedule a job to be enqueued very soon
    enqueue_at = datetime.utcnow() + timedelta(seconds=1)
    job = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1000, 2000]),
        kwargs=json.dumps({}),
        state_name=ScheduledState.NAME
    )
    redis_storage.schedule(job, enqueue_at)

    worker = Worker(redis_storage, json_serializer, queues=["default"], scheduler_poll_interval_seconds=0.5)
    
    worker_thread = Thread(target=worker.run)
    worker_thread.start()
    time.sleep(3) # Give worker time to run scheduler and process job
    worker._shutdown_requested = True # Signal shutdown
    worker_thread.join()

    updated_job = redis_storage.get_job_data(job.id)
    assert updated_job.state_name == SucceededState.NAME
    assert updated_job.state_data["result"] == 3000
    assert redis_client.zcard("pytaskflow:scheduled") == 0 # Should be removed from scheduled

def test_worker_processes_recurring_job(redis_storage, json_serializer, redis_client):
    recurring_job_id = "test_recurring_job_worker"
    job_template = Job(
        target_module="tests.test_tasks",
        target_function="success_task",
        args=json.dumps([1, 1]),
        kwargs=json.dumps({}),
        state_name=""
    )
    # Schedule to run every second for testing
    redis_storage.add_recurring_job(recurring_job_id, job_template, "* * * * * *") # Every second

    worker = Worker(redis_storage, json_serializer, queues=["default"], scheduler_poll_interval_seconds=0.5)
    
    worker_thread = Thread(target=worker.run)
    worker_thread.start()
    time.sleep(3) # Allow multiple runs
    worker._shutdown_requested = True
    worker_thread.join()

    # Check if at least one job was enqueued and processed
    # This is a bit tricky to assert exact count due to timing, but we can check for success
    # We'll check the last_execution time in the recurring job data
    stored_data = redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id)
    assert stored_data is not None
    data = json.loads(stored_data.decode())
    assert data["last_execution"] is not None

    # We can't easily get the job_id of the triggered job, so we'll check the queue directly
    # The worker processes jobs, so the queue should be empty or nearly empty
    assert redis_client.llen("pytaskflow:queue:default") == 0

    # To verify a job was processed, we'd need to inspect logs or a more complex state tracking
    # For now, the `last_execution` update is a good indicator that the scheduler ran.
