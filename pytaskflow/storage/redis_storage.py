# pytaskflow/storage/redis_storage.py
import redis
import json
import logging
from typing import Optional, List, Any
from datetime import datetime, UTC
import time

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState, EnqueuedState

logger = logging.getLogger(__name__)


class RedisStorage(JobStorage):
    def __init__(self, connection_pool=None, redis_client=None):
        if redis_client:
            self.redis_client = redis_client
            # Ensure decode_responses is enabled if not already set
            if not getattr(self.redis_client, "decode_responses", False):
                # Create a new client with decode_responses=True using the same connection pool
                self.redis_client = redis.Redis(
                    connection_pool=self.redis_client.connection_pool,
                    decode_responses=True,
                )
        elif connection_pool:
            self.redis_client = redis.Redis(
                connection_pool=connection_pool, decode_responses=True
            )
        else:
            self.redis_client = redis.Redis(
                host="localhost", port=6379, db=0, decode_responses=True
            )

        # Lua script for moving scheduled jobs to an enqueued state
        self.move_to_enqueued_script = self.redis_client.register_script("""
            local scheduled_key = KEYS[1]
            local now_timestamp = tonumber(ARGV[1])
            local state_name = ARGV[2]
            local due_jobs = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_timestamp, 'LIMIT', 0, 100)
            if #due_jobs == 0 then
                return nil
            end
            for i=1, #due_jobs do
                local job_id = due_jobs[i]
                local job_key = 'pytaskflow:job:' .. job_id
                local queue = redis.call('HGET', job_key, 'queue')
                if not queue then queue = 'default' end
                if redis.call('ZREM', scheduled_key, job_id) > 0 then
                    redis.call('HSET', job_key, 'state_name', state_name)
                    redis.call('HSET', job_key, 'state_data', '{"queue":"' .. queue .. '"}')
                    redis.call('LPUSH', 'pytaskflow:queue:' .. queue, job_id)
                end
            end
            return due_jobs
        """)

    def _serialize_job_for_storage(self, job: Job) -> dict:
        job_dict = {}
        for key, value in job.__dict__.items():
            if isinstance(value, datetime):
                job_dict[key] = value.isoformat()
            elif isinstance(value, dict):
                job_dict[key] = json.dumps(value)
            else:
                job_dict[key] = str(value)
        return job_dict

    def _deserialize_job_from_storage(self, job_data: dict) -> Job:
        job_dict = {}
        for key, value in job_data.items():
            # Ensure key is a string (decode if it's bytes)
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            # Ensure value is a string (decode if it's bytes)
            if isinstance(value, bytes):
                value = value.decode("utf-8")

            if key == "created_at":
                job_dict[key] = datetime.fromisoformat(value)
            elif key == "state_data":
                try:
                    job_dict[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    job_dict[key] = {}
            elif key == "retry_count":
                job_dict[key] = int(value)
            else:
                job_dict[key] = value
        return Job(**job_dict)

    def enqueue(self, job: Job) -> str:
        with self.redis_client.pipeline() as pipe:
            job_key = f"pytaskflow:job:{job.id}"
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.lpush(f"pytaskflow:queue:{job.queue}", job.id)
            pipe.execute()
        return job.id

    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        job_key = f"pytaskflow:job:{job.id}"
        scheduled_key = "pytaskflow:scheduled"
        score = enqueue_at.timestamp()
        with self.redis_client.pipeline() as pipe:
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.zadd(scheduled_key, {job.id: score})
            pipe.execute()
        return job.id

    def add_recurring_job(
        self, recurring_job_id: str, job_template: Job, cron_expression: str
    ):
        job_dict = self._serialize_job_for_storage(job_template)
        data = {"job": job_dict, "cron": cron_expression, "last_execution": None}
        with self.redis_client.pipeline() as pipe:
            pipe.hset(
                "pytaskflow:recurring-jobs",
                recurring_job_id,
                json.dumps(data, default=str),
            )
            pipe.sadd("pytaskflow:recurring-jobs:ids", recurring_job_id)
            pipe.execute()

    def remove_recurring_job(self, recurring_job_id: str):
        with self.redis_client.pipeline() as pipe:
            pipe.hdel("pytaskflow:recurring-jobs", recurring_job_id)
            pipe.srem("pytaskflow:recurring-jobs:ids", recurring_job_id)
            pipe.execute()

    def trigger_recurring_job(self, recurring_job_id: str):
        data_str = self.redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id)
        if not data_str:
            return
        data = json.loads(data_str)
        job_dict = data["job"]
        deserialized_job = self._deserialize_job_from_storage(job_dict)
        job_instance = Job(
            target_module=deserialized_job.target_module,
            target_function=deserialized_job.target_function,
            args=deserialized_job.args,
            kwargs=deserialized_job.kwargs,
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data(),
            queue=deserialized_job.queue,
        )
        self.enqueue(job_instance)

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        processing_list = "pytaskflow:queue:processing"
        job_id = None

        # The `brpoplpush` command is the correct primitive. We loop through the queues
        # to provide prioritized fetching. A short timeout makes the worker responsive.
        # Note: A single `brpoplpush` with a long timeout would "starve" other queues.
        # This iterative approach is the standard, correct pattern.
        start_time = time.monotonic()
        while True:
            for queue in queues:
                source_queue = f"pytaskflow:queue:{queue}"
                try:
                    job_id = self.redis_client.brpoplpush(
                        source_queue, processing_list, timeout=1
                    )
                    if job_id:
                        break  # Found a job, exit the inner loop
                except redis.exceptions.TimeoutError:
                    continue  # Go to the next queue

            if job_id:
                break  # Exit the outer loop to process the job

            # Check if the total timeout has been exceeded
            if (time.monotonic() - start_time) > timeout_seconds:
                return None

        # Decode job_id if it's bytes
        if isinstance(job_id, bytes):
            job_id = job_id.decode("utf-8")

        processing_state = ProcessingState("server-redis", "worker-1")
        self.set_job_state(job_id, processing_state)
        return self.get_job_data(job_id)

    def acknowledge(self, job_id: str) -> None:
        self.redis_client.lrem("pytaskflow:queue:processing", 1, job_id)

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        job_key = f"pytaskflow:job:{job_id}"

        if expected_old_state:
            # Use optimistic locking with WATCH/MULTI/EXEC
            with self.redis_client.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        pipe.watch(job_key)
                        current_state = pipe.hget(job_key, "state_name")
                        # Decode if bytes
                        if isinstance(current_state, bytes):
                            current_state = current_state.decode("utf-8")

                        if current_state != expected_old_state:
                            pipe.unwatch()
                            return False

                        pipe.multi()
                        pipe.hset(job_key, "state_name", state.name)
                        pipe.hset(
                            job_key,
                            "state_data",
                            json.dumps(state.serialize_data(), default=str),
                        )

                        # If job is being re-enqueued for retry, move it from processing back to its queue
                        if isinstance(state, EnqueuedState):
                            job_data = self.get_job_data(job_id)
                            if job_data:
                                pipe.lrem("pytaskflow:queue:processing", 1, job_id)
                                pipe.lpush(f"pytaskflow:queue:{job_data.queue}", job_id)

                        pipe.execute()
                        break
                    except redis.exceptions.WatchError:
                        # Retry the operation
                        continue
            return True
        else:
            # Simple case without optimistic locking
            with self.redis_client.pipeline() as pipe:
                pipe.hset(job_key, "state_name", state.name)
                pipe.hset(
                    job_key,
                    "state_data",
                    json.dumps(state.serialize_data(), default=str),
                )

                # If job is being re-enqueued for retry, move it from processing back to its queue
                if isinstance(state, EnqueuedState):
                    job_data = self.get_job_data(job_id)
                    if job_data:
                        pipe.lrem("pytaskflow:queue:processing", 1, job_id)
                        pipe.lpush(f"pytaskflow:queue:{job_data.queue}", job_id)

                pipe.execute()
            return True

    def get_job_data(self, job_id: str) -> Optional[Job]:
        job_key = f"pytaskflow:job:{job_id}"
        job_data = self.redis_client.hgetall(job_key)
        if not job_data:
            return None
        return self._deserialize_job_from_storage(job_data)

    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None:
        job_key = f"pytaskflow:job:{job_id}"
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        elif isinstance(value, datetime):
            value = value.isoformat()
        else:
            value = str(value)
        self.redis_client.hset(job_key, field_name, value)
