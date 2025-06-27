# pytaskflow/storage/redis_storage.py
import redis
import json
import logging
from typing import Optional, List, Any
from datetime import datetime, UTC

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState, EnqueuedState

logger = logging.getLogger(__name__)

class RedisStorage(JobStorage):
    def __init__(self, connection_pool=None, redis_client=None):
        if redis_client:
            self.redis_client = redis_client
        elif connection_pool:
            self.redis_client = redis.Redis(connection_pool=connection_pool)
        else:
            # Default connection for ease of use
            self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

        # Load Lua script for moving scheduled jobs to an enqueued state
        # This remains an efficient way to handle scheduled jobs.
        self.move_to_enqueued_script = self.redis_client.register_script("""
            local scheduled_key = KEYS[1]
            local now_timestamp = tonumber(ARGV[1])
            local state_name = ARGV[2]
            
            -- Get up to 100 due jobs to enqueue in one go
            local due_jobs = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_timestamp, 'LIMIT', 0, 100)
            
            if #due_jobs == 0 then
                return nil
            end
            
            for i=1, #due_jobs do
                local job_id = due_jobs[i]
                local job_key = 'pytaskflow:job:' .. job_id
                local queue = redis.call('HGET', job_key, 'queue')
                if not queue then queue = 'default' end

                -- Atomically remove from scheduled set and push to queue
                if redis.call('ZREM', scheduled_key, job_id) > 0 then
                    redis.call('HSET', job_key, 'state_name', state_name)
                    redis.call('HSET', job_key, 'state_data', '{"queue":"' .. queue .. '"}')
                    redis.call('LPUSH', 'pytaskflow:queue:' .. queue, job_id)
                end
            end
            
            return due_jobs
        """)

    def _serialize_job_for_storage(self, job: Job) -> dict:
        """Convert job to a dict with all values as strings for Redis storage."""
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
        """Convert Redis hash data back to Job object."""
        job_dict = {}
        for key, value in job_data.items():
            key_str = key.decode()
            value_str = value.decode()

            if key_str == "created_at":
                job_dict[key_str] = datetime.fromisoformat(value_str)
            elif key_str == "state_data":
                try:
                    job_dict[key_str] = json.loads(value_str)
                except json.JSONDecodeError:
                    job_dict[key_str] = {} # Handle empty or invalid JSON
            elif key_str == "retry_count":
                job_dict[key_str] = int(value_str)
            else:
                job_dict[key_str] = value_str

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
            pipe.hset("pytaskflow:recurring-jobs", recurring_job_id, json.dumps(data))
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

        data = json.loads(data_str.decode())
        job_dict = data["job"]
        
        # Deserialize from stored strings back to correct types for Job constructor
        deserialized_job_dict = self._deserialize_job_from_storage({k.encode(): v.encode() for k,v in job_dict.items()})

        job_instance = Job(
            target_module=deserialized_job_dict.target_module,
            target_function=deserialized_job_dict.target_function,
            args=deserialized_job_dict.args,
            kwargs=deserialized_job_dict.kwargs,
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data(),
            queue=deserialized_job_dict.queue,
        )
        self.enqueue(job_instance)

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        """
        Atomically fetches a job from one of the specified queues and places it
        into a reliable 'processing' list to prevent job loss on worker failure.
        
        This uses `brpoplpush`, which is the correct high-level command in redis-py
        for this pattern. It transparently uses BLMOVE on Redis 6.2+.

        Using brpoplpush instead of running brpop and then lpush ensures atomicity, atomically removes an element from one list and pushes it to another (a processing/backup list).
        Otherwise we risk losing the job if the worker fails/crashes between the two commands (brpop and lpush).

        This is a common pattern in Redis for reliable job processing.
        """
        source_queues = [f"pytaskflow:queue:{q}" for q in queues]
        processing_list = "pytaskflow:queue:processing"
        
        try:
            job_id_bytes = self.redis_client.brpoplpush(
                source_queues, processing_list, timeout=timeout_seconds
            )
        except redis.exceptions.TimeoutError:
            return None # Expected when no job is available

        if not job_id_bytes:
            return None # No job found within the timeout

        job_id = job_id_bytes.decode()

        # The job is now safely in the processing list. Update its state.
        processing_state = ProcessingState("server-redis", "worker-1")
        self.set_job_state(job_id, processing_state)

        return self.get_job_data(job_id)

    def acknowledge(self, job_id: str) -> None:
        """Removes a job from the processing list upon successful completion or permanent failure."""
        self.redis_client.lrem("pytaskflow:queue:processing", 1, job_id)

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        job_key = f"pytaskflow:job:{job_id}"

        with self.redis_client.pipeline(transaction=True) as pipe:
            if expected_old_state:
                pipe.watch(job_key)
                current_state_bytes = pipe.hget(job_key, "state_name")
                current_state = current_state_bytes.decode() if current_state_bytes else None
                
                if current_state != expected_old_state:
                    pipe.unwatch()
                    return False
            
            pipe.multi()
            pipe.hset(job_key, "state_name", state.name)
            pipe.hset(job_key, "state_data", json.dumps(state.serialize_data(), default=str))
            
            try:
                pipe.execute()
            except redis.exceptions.WatchError:
                logger.warning(f"WatchError on job {job_id} during state transition.")
                return False

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
