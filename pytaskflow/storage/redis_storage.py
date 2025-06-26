# pytaskflow/storage/redis_storage.py
import redis
import json
from typing import Optional, List
from datetime import datetime

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState, EnqueuedState

class RedisStorage(JobStorage):
    def __init__(self, connection_pool: Optional[redis.ConnectionPool] = None, **redis_options):
        if connection_pool:
            self.redis_client = redis.Redis(connection_pool=connection_pool)
        else:
            self.redis_client = redis.Redis(**redis_options)
        # Lua script for atomically moving a job from scheduled to enqueued
        self.move_to_enqueued_script = self.redis_client.script_load("""
            local job_id = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)[1]
            if job_id then
                redis.call('zrem', KEYS[1], job_id)
                redis.call('hset', 'pytaskflow:job:' .. job_id, 'state_name', ARGV[2])
                redis.call('lpush', 'pytaskflow:queue:' .. ARGV[3], job_id)
                return job_id
            end
            return nil
        """)

    def enqueue(self, job: Job) -> str:
        with self.redis_client.pipeline() as pipe:
            job_key = f"pytaskflow:job:{job.id}"
            # Using __dict__ from the dataclass for simplicity
            pipe.hset(job_key, mapping=job.__dict__)
            pipe.lpush(f"pytaskflow:queue:{job.queue}", job.id)
            pipe.execute()
        return job.id

    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        job_key = f"pytaskflow:job:{job.id}"
        scheduled_key = "pytaskflow:scheduled"
        
        # Convert datetime to UNIX timestamp for the score
        score = enqueue_at.timestamp()
        
        with self.redis_client.pipeline() as pipe:
            pipe.hset(job_key, mapping=job.__dict__)
            pipe.zadd(scheduled_key, {job.id: score})
            pipe.execute()
        return job.id
    
    def add_recurring_job(self, recurring_job_id: str, job_template: Job, cron_expression: str):
        # Serialize the entire job definition and options into a JSON string
        data = {
            "job": job_template.__dict__,
            "cron": cron_expression,
            "last_execution": None # Or current time
        }
        self.redis_client.hset("pytaskflow:recurring-jobs", recurring_job_id, json.dumps(data))
        self.redis_client.sadd("pytaskflow:recurring-jobs:ids", recurring_job_id)

    def remove_recurring_job(self, recurring_job_id: str):
        self.redis_client.hdel("pytaskflow:recurring-jobs", recurring_job_id)
        self.redis_client.srem("pytaskflow:recurring-jobs:ids", recurring_job_id)

    def trigger_recurring_job(self, recurring_job_id: str):
        # Fetch the recurring job template
        data_str = self.redis_client.hget("pytaskflow:recurring-jobs", recurring_job_id)
        if not data_str:
            # Recurring job doesn't exist, nothing to trigger
            return

        data = json.loads(data_str.decode())
        job_dict = data["job"]
        
        # Create a new job instance from the template and enqueue it immediately
        job_instance = Job(
            target_module=job_dict["target_module"],
            target_function=job_dict["target_function"],
            args=job_dict["args"],
            kwargs=job_dict["kwargs"],
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data(),
            queue=job_dict.get("queue", "default")
        )
        self.enqueue(job_instance)

    def dequeue(self, queues: List[str], timeout_seconds: int) -> Optional[Job]:
        # BRPOPLPUSH is atomic and reliable. It moves an item from one list to another.
        # If the worker crashes, the job remains in the processing list.
        queue_keys = [f"pytaskflow:queue:{q}" for q in queues]
        
        # This command blocks until a job is available or the timeout is reached.
        job_id = self.redis_client.brpoplpush(queue_keys, "pytaskflow:queue:processing:temp", timeout=timeout_seconds)
        
        if not job_id:
            return None
        
        job_id = job_id.decode() # redis-py returns bytes
        job_key = f"pytaskflow:job:{job_id}"
        
        # Rename the temporary processing item to a worker-specific one
        # This will be refined in later phases. For now, it's just 'processing'.
        self.redis_client.lrem("pytaskflow:queue:processing:temp", 1, job_id)
        self.redis_client.lpush(f"pytaskflow:queue:processing", job_id)

        job_data = self.redis_client.hgetall(job_key)
        
        if not job_data:
            # Job data expired or was deleted between dequeue and fetch
            self.redis_client.lrem(f"pytaskflow:queue:processing", 1, job_id)
            return None
            
        # Decode bytes from redis
        job_dict = {k.decode(): v.decode() for k, v in job_data.items()}
        
        # Update state to Processing
        self.set_job_state(job_id, ProcessingState("server-redis", "worker-1"))
        
        return Job(**job_dict)

    def acknowledge(self, job_id: str) -> None:
        # Remove from the processing list
        self.redis_client.lrem(f"pytaskflow:queue:processing", 1, job_id)

    def set_job_state(self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None) -> bool:
        job_key = f"pytaskflow:job:{job_id}"
        with self.redis_client.pipeline() as pipe:
            if expected_old_state:
                pipe.watch(job_key)
                current_state = pipe.hget(job_key, "state_name").decode()
                if current_state != expected_old_state:
                    pipe.unwatch()
                    return False
            pipe.multi()
            pipe.hset(job_key, "state_name", state.name)
            pipe.hset(job_key, "state_data", json.dumps(state.serialize_data()))
            pipe.execute()
        return True

    def get_job_data(self, job_id: str) -> Optional[Job]:
        job_key = f"pytaskflow:job:{job_id}"
        job_data = self.redis_client.hgetall(job_key)
        if not job_data:
            return None
        job_dict = {k.decode(): v.decode() for k, v in job_data.items()}
        return Job(**job_dict)
