# pytaskflow/storage/redis_storage.py
import redis
import json
import uuid
from typing import Optional, List, Any
from datetime import datetime, UTC

from .base import JobStorage
from ..common.job import Job
from ..common.states import BaseState, ProcessingState, EnqueuedState

class RedisStorage(JobStorage):
    def __init__(self, connection_pool=None, redis_client=None):
        if redis_client:
            self.redis_client = redis_client
        elif connection_pool:
            self.redis_client = redis.Redis(connection_pool=connection_pool)
        else:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        # Load Lua script for moving scheduled jobs
        self.move_to_enqueued_script = self.redis_client.register_script("""
            local scheduled_key = KEYS[1]
            local now_timestamp = tonumber(ARGV[1])
            local state_name = ARGV[2]
            local queue = ARGV[3]
            
            -- Get jobs that are due (score <= now_timestamp)
            local due_jobs = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_timestamp, 'LIMIT', 0, 1)
            
            if #due_jobs == 0 then
                return nil
            end
            
            local job_id = due_jobs[1]
            
            -- Remove from scheduled set
            redis.call('ZREM', scheduled_key, job_id)
            
            -- Update job state and add to queue
            redis.call('HSET', 'pytaskflow:job:' .. job_id, 'state_name', state_name)
            redis.call('HSET', 'pytaskflow:job:' .. job_id, 'state_data', '{"queue":"' .. queue .. '"}')
            redis.call('LPUSH', 'pytaskflow:queue:' .. queue, job_id)
            
            return job_id
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
            key_str = key.decode() if isinstance(key, bytes) else key
            value_str = value.decode() if isinstance(value, bytes) else value
            
            if key_str == 'created_at':
                job_dict[key_str] = datetime.fromisoformat(value_str)
            elif key_str == 'state_data':
                job_dict[key_str] = json.loads(value_str)
            elif key_str == 'retry_count':
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
        
        # Convert datetime to UNIX timestamp for the score
        score = enqueue_at.timestamp()
        
        with self.redis_client.pipeline() as pipe:
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.zadd(scheduled_key, {job.id: score})
            pipe.execute()
        return job.id

    def add_recurring_job(self, recurring_job_id: str, job_template: Job, cron_expression: str):
        # Convert job template to dict and handle datetime serialization
        job_dict = job_template.__dict__.copy()
        if isinstance(job_dict.get('created_at'), datetime):
            job_dict['created_at'] = job_dict['created_at'].isoformat()
        
        data = {
            "job": job_dict,
            "cron": cron_expression,
            "last_execution": None
        }
        
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
        
        # Create a new job instance from the template
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
        # Use BRPOPLPUSH to atomically move job from queue to processing
        if timeout_seconds == 0:
            timeout_seconds = None
        
        queue_keys = [f"pytaskflow:queue:{q}" for q in queues]
        result = self.redis_client.brpop(queue_keys, timeout=timeout_seconds)
        
        if not result:
            return None
        
        queue_key, job_id_bytes = result
        job_id = job_id_bytes.decode()
        
        # Move to processing and update state
        with self.redis_client.pipeline() as pipe:
            pipe.lpush("pytaskflow:queue:processing", job_id)
            pipe.hset(f"pytaskflow:job:{job_id}", "state_name", ProcessingState.NAME)
            pipe.hset(f"pytaskflow:job:{job_id}", "state_data", json.dumps({"server_id": "server-mvp", "worker_id": "worker-1"}))
            pipe.execute()
        
        return self.get_job_data(job_id)

    def acknowledge(self, job_id: str) -> None:
        self.redis_client.lrem("pytaskflow:queue:processing", 1, job_id)

    def set_job_state(self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None) -> bool:
        job_key = f"pytaskflow:job:{job_id}"
        
        # If expected_old_state is specified, check it first
        if expected_old_state:
            current_state = self.redis_client.hget(job_key, "state_name")
            if not current_state or current_state.decode() != expected_old_state:
                return False
        
        # Update state
        with self.redis_client.pipeline() as pipe:
            pipe.hset(job_key, "state_name", state.name)
            pipe.hset(job_key, "state_data", json.dumps(state.serialize_data()))
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
