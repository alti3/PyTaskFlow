# pytaskflow/server/worker.py
import asyncio
import json
import logging
import time
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

from cronsim import CronSim

from pytaskflow.storage.base import JobStorage
from pytaskflow.storage.redis_storage import RedisStorage
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.server.processor import JobProcessor
from pytaskflow.serialization.json_serializer import JsonSerializer
from ..common.states import EnqueuedState
from ..common.job import Job

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ConcurrencyMode(Enum):
    THREADED = "threaded"
    ASYNCIO = "asyncio"


class Worker:
    def __init__(
        self,
        storage: JobStorage,
        serializer: Optional[BaseSerializer] = None,
        queues: List[str] = ["default"],
        worker_count: int = 4,
        scheduler_poll_interval_seconds: int = 15,
        concurrency_mode: ConcurrencyMode = ConcurrencyMode.THREADED,
    ):
        self.storage = storage
        self.serializer = serializer or JsonSerializer()
        self.queues = queues
        self.worker_count = worker_count
        self.server_id = f"server:{uuid.uuid4()}"
        self.worker_id = f"worker:{uuid.uuid4()}"
        self._shutdown_requested = False
        self.scheduler_poll_interval = scheduler_poll_interval_seconds
        self._last_scheduler_run = 0
        self._last_heartbeat = 0
        self.concurrency_mode = concurrency_mode

    def _run_schedulers(self):
        """Runs periodic tasks like enqueuing scheduled jobs."""
        now = time.monotonic()
        if (now - self._last_scheduler_run) < self.scheduler_poll_interval:
            return

        logger.info(f"[{self.worker_id}] Running schedulers...")
        self._enqueue_due_scheduled_jobs()
        self._enqueue_due_recurring_jobs()

        self._last_scheduler_run = now

    def _enqueue_due_scheduled_jobs(self):
        storage = self.storage
        if hasattr(storage, "enqueue_due_scheduled_jobs"):
            storage.enqueue_due_scheduled_jobs()
            return
        if not isinstance(storage, RedisStorage):  # Phase 2 only supports Redis
            return

        now_timestamp = datetime.now(UTC).timestamp()

        while True:
            # Using the Lua script to atomically move jobs
            job_ids_result = storage.move_to_enqueued_script(
                keys=["pytaskflow:scheduled"],
                args=[
                    now_timestamp,
                    EnqueuedState.NAME,
                    datetime.now(UTC).isoformat(),
                ],
            )

            if not job_ids_result:
                break  # No more jobs to enqueue

            # The script returns a list of job IDs, iterate through them
            for job_id in job_ids_result:
                logger.info(
                    f"[{self.worker_id}] Moved scheduled job {job_id} to enqueued."
                )

    def _enqueue_due_recurring_jobs(self):
        # This entire method should be protected by a distributed lock to ensure only one worker
        # runs the recurring job scheduler at a time.
        if hasattr(self.storage, "enqueue_due_recurring_jobs"):
            self.storage.enqueue_due_recurring_jobs()
            return
        if not isinstance(self.storage, RedisStorage):
            return

        lock_key = "pytaskflow:lock:recurring-scheduler"
        if not self.storage.redis_client.set(lock_key, self.server_id, ex=60, nx=True):
            return  # Another worker is handling it

        try:
            now = datetime.now(UTC)
            recurring_job_ids = self.storage.redis_client.smembers(
                "pytaskflow:recurring-jobs:ids"
            )

            for job_id in recurring_job_ids:
                # Use a distributed lock per job to handle updates atomically
                job_lock_key = f"pytaskflow:lock:recurring-job:{job_id}"
                if not self.storage.redis_client.set(
                    job_lock_key, self.server_id, ex=10, nx=True
                ):
                    continue

                try:
                    data_str = self.storage.redis_client.hget(
                        "pytaskflow:recurring-jobs", job_id
                    )
                    if not data_str:
                        continue

                    data = json.loads(data_str)

                    last_execution_str = data.get("last_execution")
                    last_execution = (
                        datetime.fromisoformat(last_execution_str)
                        if last_execution_str
                        else now
                    )

                    cron = CronSim(data["cron"], last_execution)
                    next_execution = next(cron)

                    if next_execution <= now:
                        # It's time to run!
                        logger.info(
                            f"[{self.worker_id}] Triggering recurring job {job_id}"
                        )
                        job_dict = data["job"]
                        # Create a new, unique job instance from the template
                        job_instance = Job(
                            target_module=job_dict["target_module"],
                            target_function=job_dict["target_function"],
                            args=job_dict["args"],
                            kwargs=job_dict["kwargs"],
                            state_name=EnqueuedState.NAME,
                            state_data=EnqueuedState().serialize_data(),
                            queue=job_dict.get("queue", "default"),
                            # recurring_job_id=job_id # Link it back
                        )
                        self.storage.enqueue(job_instance)

                        # Update last execution time
                        data["last_execution"] = now.isoformat()
                        self.storage.redis_client.hset(
                            "pytaskflow:recurring-jobs", job_id, json.dumps(data)
                        )
                finally:
                    self.storage.redis_client.delete(job_lock_key)

        finally:
            self.storage.redis_client.delete(lock_key)

    def _send_heartbeat(self):
        now = time.monotonic()
        if (now - self._last_heartbeat) < 30:
            return
        self.storage.server_heartbeat(self.server_id, self.worker_count, self.queues)
        self._last_heartbeat = now

    def run(self):
        """Starts the worker's processing loop."""
        if self.concurrency_mode == ConcurrencyMode.THREADED:
            self._run_threaded()
            return
        if self.concurrency_mode == ConcurrencyMode.ASYNCIO:
            asyncio.run(self._run_async())
            return
        raise ValueError(f"Unsupported concurrency mode: {self.concurrency_mode}")

    def _run_threaded(self):
        logger.info(
            f"[{self.worker_id}] Starting worker for queues: {', '.join(self.queues)}"
        )
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            in_flight_futures: set = set()
            while not self._shutdown_requested:
                try:
                    self._send_heartbeat()
                    self._run_schedulers()

                    while len(in_flight_futures) < self.worker_count:
                        dequeued_job = self.storage.dequeue(
                            self.queues,
                            timeout_seconds=0.1,
                            server_id=self.server_id,
                            worker_id=self.worker_id,
                        )
                        if not dequeued_job:
                            break

                        logger.info(
                            f"[{self.worker_id}] Picked up job {dequeued_job.id} (state: {dequeued_job.state_name}, retry_count: {dequeued_job.retry_count})"
                        )
                        processor = JobProcessor(
                            dequeued_job, self.storage, self.serializer
                        )
                        in_flight_futures.add(executor.submit(processor.process))

                    if not in_flight_futures:
                        time.sleep(0.05)
                        continue

                    try:
                        done_futures = set(
                            f for f in as_completed(in_flight_futures, timeout=0.1)
                        )
                    except TimeoutError:
                        done_futures = set()

                    for future in done_futures:
                        in_flight_futures.remove(future)
                        try:
                            future.result()
                        except Exception as exc:
                            logger.error(
                                f"[{self.worker_id}] Job processor task failed: {exc}",
                                exc_info=True,
                            )

                except KeyboardInterrupt:
                    logger.info(f"[{self.worker_id}] Shutdown requested...")
                    self._shutdown_requested = True
                except Exception as e:
                    logger.error(
                        f"[{self.worker_id}] Unhandled exception in worker loop: {e}",
                        exc_info=True,
                    )
                    time.sleep(5)  # Cooldown period after a major failure

            for future in as_completed(in_flight_futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.error(
                        f"[{self.worker_id}] Job processor task failed during shutdown: {exc}",
                        exc_info=True,
                    )

        self.storage.remove_server(self.server_id)
        logger.info(f"[{self.worker_id}] Worker has stopped.")

    async def _run_async(self):
        logger.info(
            f"[{self.worker_id}] Starting ASYNC worker for queues: {', '.join(self.queues)}"
        )
        in_flight_tasks: set[asyncio.Task] = set()

        while not self._shutdown_requested:
            try:
                self._send_heartbeat()
                self._run_schedulers()

                while len(in_flight_tasks) < self.worker_count:
                    dequeued_job = await asyncio.to_thread(
                        self.storage.dequeue,
                        self.queues,
                        0.1,
                        self.server_id,
                        self.worker_id,
                    )
                    if not dequeued_job:
                        break

                    logger.info(
                        f"[{self.worker_id}] Picked up job {dequeued_job.id} (state: {dequeued_job.state_name}, retry_count: {dequeued_job.retry_count})"
                    )
                    processor = JobProcessor(
                        dequeued_job, self.storage, self.serializer
                    )
                    task = asyncio.create_task(processor.process_async())
                    in_flight_tasks.add(task)

                if not in_flight_tasks:
                    await asyncio.sleep(0.05)
                    continue

                done, _pending = await asyncio.wait(
                    in_flight_tasks,
                    timeout=0.1,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in done:
                    in_flight_tasks.discard(task)
                    try:
                        task.result()
                    except Exception as exc:
                        logger.error(
                            f"[{self.worker_id}] Job processor task failed: {exc}",
                            exc_info=True,
                        )

            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.info(f"[{self.worker_id}] Shutdown requested...")
                self._shutdown_requested = True
            except Exception as e:
                logger.error(
                    f"[{self.worker_id}] Unhandled exception in async worker loop: {e}",
                    exc_info=True,
                )
                await asyncio.sleep(5)

        if in_flight_tasks:
            results = await asyncio.gather(*in_flight_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(
                        f"[{self.worker_id}] Job processor task failed during shutdown: {result}",
                        exc_info=True,
                    )

        self.storage.remove_server(self.server_id)
        logger.info(f"[{self.worker_id}] Worker has stopped.")
