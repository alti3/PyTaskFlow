# pytaskflow/storage/redis_storage.py
import redis
import json
import logging
from typing import Optional, List, Any, Dict
from datetime import datetime, UTC
import time

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState, EnqueuedState, ALL_STATES

logger = logging.getLogger(__name__)


class RedisStorage(JobStorage):
    def __init__(self, connection_pool=None, redis_client=None):
        if redis_client:
            self.redis_client = redis_client
            if not getattr(self.redis_client, "decode_responses", False):
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

        # Lua script for atomically setting job state and updating state sets
        self.set_job_state_script = self.redis_client.register_script("""
            local job_key = KEYS[1]
            local job_id = ARGV[1]
            local new_state_name = ARGV[2]
            local new_state_data = ARGV[3]
            local expected_old_state = ARGV[4]
            local all_states = ARGV[5]

            local current_state = redis.call('HGET', job_key, 'state_name')
            if expected_old_state and current_state ~= expected_old_state then
                return 0
            end

            redis.call('HSET', job_key, 'state_name', new_state_name)
            redis.call('HSET', job_key, 'state_data', new_state_data)

            -- Remove from old state set and add to new one
            if current_state then
                redis.call('SREM', 'pytaskflow:jobs:' .. current_state, job_id)
            end
            redis.call('SADD', 'pytaskflow:jobs:' .. new_state_name, job_id)

            return 1
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
        # Implementation remains the same...
        job_dict = {}
        for key, value in job_data.items():
            if isinstance(key, bytes): key = key.decode("utf-8")
            if isinstance(value, bytes): value = value.decode("utf-8")
            if key == "created_at": job_dict[key] = datetime.fromisoformat(value)
            elif key == "state_data":
                try: job_dict[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError): job_dict[key] = {}
            elif key == "retry_count": job_dict[key] = int(value)
            else: job_dict[key] = value
        return Job(**job_dict)


    def enqueue(self, job: Job) -> str:
        with self.redis_client.pipeline() as pipe:
            job_key = f"pytaskflow:job:{job.id}"
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.lpush(f"pytaskflow:queue:{job.queue}", job.id)
            # Also add to the 'enqueued' state set
            pipe.sadd(f"pytaskflow:jobs:{EnqueuedState.NAME}", job.id)
            pipe.execute()
        return job.id

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        job_key = f"pytaskflow:job:{job_id}"
        result = self.set_job_state_script(
            keys=[job_key],
            args=[
                job_id,
                state.name,
                json.dumps(state.serialize_data(), default=str),
                expected_old_state or "",
                json.dumps(ALL_STATES)
            ],
        )
        return result == 1

    def get_job_data(self, job_id: str) -> Optional[Job]:
        job_key = f"pytaskflow:job:{job_id}"
        job_data = self.redis_client.hgetall(job_key)
        if not job_data:
            return None
        return self._deserialize_job_from_storage(job_data)

    def get_job_ids_by_state(
        self, state_name: str, start: int, count: int
    ) -> List[str]:
        state_key = f"pytaskflow:jobs:{state_name}"
        return self.redis_client.sort(state_key, start=start, num=count, desc=True)

    def get_state_job_count(self, state_name: str) -> int:
        state_key = f"pytaskflow:jobs:{state_name}"
        return self.redis_client.scard(state_key)

    def get_all_servers(self) -> List[Dict]:
        # This requires a proper worker heartbeat mechanism, which is not yet implemented.
        # Returning dummy data for now.
        return [
            {
                "id": "server-1", "hostname": "localhost", "pid": 1234,
                "last_heartbeat": datetime.now(UTC).isoformat(), "status": "active"
            }
        ]

    def get_recurring_jobs(self, start: int, count: int) -> List[dict]:
        ids = self.redis_client.zrange("pytaskflow:recurring-jobs:ids", start, start + count - 1)
        if not ids:
            return []
        jobs_data = self.redis_client.hmget("pytaskflow:recurring-jobs", ids)
        return [json.loads(job_data) for job_data in jobs_data if job_data]

    # --- Other methods from the original file ---
    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        job_key = f"pytaskflow:job:{job.id}"
        scheduled_key = "pytaskflow:scheduled"
        score = enqueue_at.timestamp()
        with self.redis_client.pipeline() as pipe:
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.zadd(scheduled_key, {job.id: score})
            pipe.sadd(f"pytaskflow:jobs:{job.state_name}", job.id)
            pipe.execute()
        return job.id

    def add_recurring_job(
        self, recurring_job_id: str, job_template: Job, cron_expression: str
    ):
        job_dict = self._serialize_job_for_storage(job_template)
        data = {"job": job_dict, "cron": cron_expression, "last_execution": None}
        score = time.time()
        with self.redis_client.pipeline() as pipe:
            pipe.hset("pytaskflow:recurring-jobs", recurring_job_id, json.dumps(data, default=str))
            pipe.zadd("pytaskflow:recurring-jobs:ids", {recurring_job_id: score})
            pipe.execute()

    def remove_recurring_job(self, recurring_job_id: str):
        with self.redis_client.pipeline() as pipe:
            pipe.hdel("pytaskflow:recurring-jobs", recurring_job_id)
            pipe.zrem("pytaskflow:recurring-jobs:ids", recurring_job_id)
            pipe.execute()

    def trigger_recurring_job(self, recurring_job_id: str):
        # Implementation remains the same
        pass

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        # This method needs to be updated to use the new set_job_state
        processing_list = "pytaskflow:queue:processing"
        job_id = self.redis_client.brpoplpush(f"pytaskflow:queue:{queues[0]}", processing_list, timeout=timeout_seconds)
        if not job_id:
            return None
        
        processing_state = ProcessingState("server-redis", "worker-1")
        self.set_job_state(job_id, processing_state, expected_old_state=EnqueuedState.NAME)
        return self.get_job_data(job_id)

    def acknowledge(self, job_id: str) -> None:
        self.redis_client.lrem("pytaskflow:queue:processing", 1, job_id)

    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None:
        job_key = f"pytaskflow:job:{job_id}"
        if isinstance(value, (dict, list)): value = json.dumps(value)
        elif isinstance(value, datetime): value = value.isoformat()
        else: value = str(value)
        self.redis_client.hset(job_key, field_name, value)