# pytaskflow/storage/redis_storage.py
import redis
import json
import logging
from typing import Optional, List, Any, Dict
from datetime import datetime, UTC

from .base import JobStorage
from ..common.job import Job
from ..common.states import (
    BaseState,
    ProcessingState,
    EnqueuedState,
    ScheduledState,
    ALL_STATES,
)

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

        # Lua script for atomically setting job state, sets, history, and stats
        self.set_job_state_script = self.redis_client.register_script(f"""
            local job_key = KEYS[1]
            local job_id = ARGV[1]
            local new_state_name = ARGV[2]
            local new_state_data = ARGV[3]
            local expected_old_state = ARGV[4]
            local history_entry = ARGV[5]

            if redis.call('EXISTS', job_key) == 0 then
                return 0
            end

            local current_state = redis.call('HGET', job_key, 'state_name')
            if expected_old_state ~= "" and current_state ~= expected_old_state then
                return 0
            end

            redis.call('HSET', job_key, 'state_name', new_state_name)
            redis.call('HSET', job_key, 'state_data', new_state_data)

            -- Remove from old state set and add to new one
            if current_state then
                redis.call('SREM', 'pytaskflow:jobs:' .. current_state, job_id)
            end
            redis.call('SADD', 'pytaskflow:jobs:' .. new_state_name, job_id)

            -- Update statistics
            if current_state then
                if redis.call('HEXISTS', 'pytaskflow:stats', current_state) == 1 then
                    redis.call('HINCRBY', 'pytaskflow:stats', current_state, -1)
                end
            end
            redis.call('HINCRBY', 'pytaskflow:stats', new_state_name, 1)

            -- Append state history
            redis.call('RPUSH', 'pytaskflow:job:' .. job_id .. ':history', history_entry)

            -- If re-enqueued (e.g., retry), move it back to its queue
            if new_state_name == '{EnqueuedState.NAME}' then
                local queue_name = redis.call('HGET', job_key, 'queue')
                if not queue_name or queue_name == '' then
                    queue_name = 'default'
                end
                local processing_list = 'pytaskflow:queue:' .. queue_name .. ':processing'
                if queue_name and queue_name ~= '' then
                    redis.call('LREM', processing_list, 1, job_id)
                    redis.call('LPUSH', 'pytaskflow:queue:' .. queue_name, job_id)
                end
            end

            return 1
        """)

        # Lua script for atomically moving scheduled jobs to enqueued
        self.move_to_enqueued_script = self.redis_client.register_script(f"""
            local scheduled_key = KEYS[1]
            local now_ts = tonumber(ARGV[1])
            local enqueued_state = ARGV[2]
            local history_timestamp = ARGV[3]

            local job_id = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_ts, 'LIMIT', 0, 1)[1]
            if not job_id then
                return nil
            end

            redis.call('ZREM', scheduled_key, job_id)

            local job_key = 'pytaskflow:job:' .. job_id
            local current_state = redis.call('HGET', job_key, 'state_name')
            local queue_name = redis.call('HGET', job_key, 'queue')
            if not queue_name or queue_name == '' then
                queue_name = 'default'
            end

            redis.call('HSET', job_key, 'state_name', enqueued_state)
            redis.call('HSET', job_key, 'state_data', '{{"queue":"' .. queue_name .. '"}}')

            redis.call('SREM', 'pytaskflow:jobs:{ScheduledState.NAME}', job_id)
            redis.call('SADD', 'pytaskflow:jobs:' .. enqueued_state, job_id)

            -- Update statistics
            if current_state then
                if redis.call('HEXISTS', 'pytaskflow:stats', current_state) == 1 then
                    redis.call('HINCRBY', 'pytaskflow:stats', current_state, -1)
                end
            end
            redis.call('HINCRBY', 'pytaskflow:stats', enqueued_state, 1)

            -- Append state history
            local history_entry = '{{"state":"' .. enqueued_state .. '","timestamp":"' .. history_timestamp .. '","data":{{"queue":"' .. queue_name .. '"}}}}'
            redis.call('RPUSH', 'pytaskflow:job:' .. job_id .. ':history', history_entry)

            redis.call('LPUSH', 'pytaskflow:queue:' .. queue_name, job_id)
            return {{job_id}}
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
            if isinstance(key, bytes):
                key = key.decode("utf-8")
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

    def _record_initial_state(self, job: Job) -> None:
        history_entry = {
            "state": job.state_name,
            "timestamp": datetime.now(UTC).isoformat(),
            "data": job.state_data,
        }
        history_key = f"pytaskflow:job:{job.id}:history"
        with self.redis_client.pipeline() as pipe:
            pipe.rpush(history_key, json.dumps(history_entry, default=str))
            pipe.hincrby("pytaskflow:stats", job.state_name, 1)
            pipe.execute()

    def enqueue(self, job: Job) -> str:
        with self.redis_client.pipeline() as pipe:
            job_key = f"pytaskflow:job:{job.id}"
            job_dict = self._serialize_job_for_storage(job)
            pipe.hset(job_key, mapping=job_dict)
            pipe.lpush(f"pytaskflow:queue:{job.queue}", job.id)
            # Also add to the 'enqueued' state set
            pipe.sadd(f"pytaskflow:jobs:{EnqueuedState.NAME}", job.id)
            pipe.execute()
        self._record_initial_state(job)
        return job.id

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        job_key = f"pytaskflow:job:{job_id}"
        history_entry = json.dumps(
            {
                "state": state.name,
                "timestamp": datetime.now(UTC).isoformat(),
                "data": state.serialize_data(),
            },
            default=str,
        )
        result = self.set_job_state_script(
            keys=[job_key],
            args=[
                job_id,
                state.name,
                json.dumps(state.serialize_data(), default=str),
                expected_old_state or "",
                history_entry,
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

    def get_jobs_by_state(self, state_name: str, start: int, count: int) -> List[Job]:
        job_ids = self.get_job_ids_by_state(state_name, start, count)
        if not job_ids:
            return []
        jobs = []
        for job_id in job_ids:
            job = self.get_job_data(job_id)
            if job:
                jobs.append(job)
        return jobs

    def get_state_job_count(self, state_name: str) -> int:
        count = self.redis_client.hget("pytaskflow:stats", state_name)
        if count is not None:
            try:
                value = int(count)
                if value >= 0:
                    return value
            except (TypeError, ValueError):
                pass
        state_key = f"pytaskflow:jobs:{state_name}"
        return self.redis_client.scard(state_key)

    def get_all_servers(self) -> List[Dict]:
        return self.get_servers()

    def server_heartbeat(self, server_id: str, worker_count: int, queues: List[str]):
        server_key = f"pytaskflow:server:{server_id}"
        data = {
            "id": server_id,
            "worker_count": str(worker_count),
            "queues": json.dumps(queues),
            "last_heartbeat": datetime.now(UTC).isoformat(),
        }
        with self.redis_client.pipeline() as pipe:
            pipe.hset(server_key, mapping=data)
            pipe.expire(server_key, 60)
            pipe.sadd("pytaskflow:servers", server_id)
            pipe.execute()

    def remove_server(self, server_id: str):
        with self.redis_client.pipeline() as pipe:
            pipe.srem("pytaskflow:servers", server_id)
            pipe.delete(f"pytaskflow:server:{server_id}")
            pipe.execute()

    def get_servers(self) -> List[dict]:
        server_ids = list(self.redis_client.smembers("pytaskflow:servers"))
        servers = []
        for server_id in server_ids:
            if isinstance(server_id, bytes):
                server_id = server_id.decode("utf-8")
            data = self.redis_client.hgetall(f"pytaskflow:server:{server_id}")
            if not data:
                self.redis_client.srem("pytaskflow:servers", server_id)
                continue
            normalized = {}
            for key, value in data.items():
                if isinstance(key, bytes):
                    key = key.decode("utf-8")
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                if key == "queues":
                    try:
                        normalized[key] = json.loads(value)
                    except json.JSONDecodeError:
                        normalized[key] = []
                elif key == "worker_count":
                    try:
                        normalized[key] = int(value)
                    except ValueError:
                        normalized[key] = value
                else:
                    normalized[key] = value
            servers.append(normalized)
        return servers

    def get_recurring_jobs(self, start: int, count: int) -> List[dict]:
        ids = list(self.redis_client.smembers("pytaskflow:recurring-jobs:ids"))
        if not ids:
            return []
        ids = ids[start : start + count]
        jobs_data = self.redis_client.hmget("pytaskflow:recurring-jobs", ids)
        jobs = []
        for recurring_job_id, job_data in zip(ids, jobs_data):
            if not job_data:
                continue
            payload = json.loads(job_data)
            payload["id"] = recurring_job_id
            jobs.append(payload)
        return jobs

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
        self._record_initial_state(job)
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

        job_instance = Job(
            target_module=job_dict["target_module"],
            target_function=job_dict["target_function"],
            args=job_dict["args"],
            kwargs=job_dict["kwargs"],
            state_name=EnqueuedState.NAME,
            state_data=EnqueuedState().serialize_data(),
            queue=job_dict.get("queue", "default"),
        )
        self.enqueue(job_instance)

    def dequeue(
        self, queues: List[str], timeout_seconds: int, server_id: str, worker_id: str
    ) -> Optional[Job]:
        if not queues:
            return None

        queue_keys = [f"pytaskflow:queue:{q}" for q in queues]
        result = self.redis_client.brpop(queue_keys, timeout=timeout_seconds)
        if not result:
            return None

        _queue_key, job_id = result
        if isinstance(_queue_key, bytes):
            _queue_key = _queue_key.decode("utf-8")
        if isinstance(job_id, bytes):
            job_id = job_id.decode("utf-8")

        queue_name = _queue_key.removeprefix("pytaskflow:queue:")
        processing_list = f"pytaskflow:queue:{queue_name}:processing"
        self.redis_client.lpush(processing_list, job_id)

        processing_state = ProcessingState(server_id, worker_id)
        self.set_job_state(
            job_id, processing_state, expected_old_state=EnqueuedState.NAME
        )
        job = self.get_job_data(job_id)
        if not job:
            self.redis_client.lrem(processing_list, 1, job_id)
        return job

    def acknowledge(self, job_id: str) -> None:
        job = self.get_job_data(job_id)
        if not job:
            return
        processing_list = f"pytaskflow:queue:{job.queue}:processing"
        self.redis_client.lrem(processing_list, 1, job_id)

    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None:
        job_key = f"pytaskflow:job:{job_id}"
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        elif isinstance(value, datetime):
            value = value.isoformat()
        else:
            value = str(value)
        self.redis_client.hset(job_key, field_name, value)

    def get_job_history(self, job_id: str) -> List[dict]:
        history_key = f"pytaskflow:job:{job_id}:history"
        entries = self.redis_client.lrange(history_key, 0, -1)
        history = []
        for entry in entries:
            if isinstance(entry, bytes):
                entry = entry.decode("utf-8")
            try:
                history.append(json.loads(entry))
            except json.JSONDecodeError:
                continue
        return history

    def get_statistics(self) -> dict:
        counts = {
            state: self.redis_client.scard(f"pytaskflow:jobs:{state}")
            for state in ALL_STATES
        }
        with self.redis_client.pipeline() as pipe:
            pipe.hset("pytaskflow:stats", mapping=counts)
            pipe.execute()
        return counts
