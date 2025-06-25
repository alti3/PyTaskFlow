# pytaskflow/server/worker.py
import time
import uuid
from typing import List

from pytaskflow.storage.base import JobStorage
from pytaskflow.serialization.base import BaseSerializer
from pytaskflow.server.processor import JobProcessor
from pytaskflow.serialization.json_serializer import JsonSerializer

class Worker:
    def __init__(self, storage: JobStorage, serializer: BaseSerializer = None, queues: List[str] = ["default"]):
        self.storage = storage
        self.serializer = serializer or JsonSerializer()
        self.queues = queues
        self.server_id = f"server:{uuid.uuid4()}"
        self.worker_id = f"worker:{uuid.uuid4()}"
        self._shutdown_requested = False

    def run(self):
        """Starts the worker's processing loop."""
        print(f"[{self.worker_id}] Starting worker for queues: {', '.join(self.queues)}")
        while not self._shutdown_requested:
            try:
                # 1. Dequeue a job
                job = self.storage.dequeue(self.queues, timeout_seconds=1)
                
                if job:
                    print(f"[{self.worker_id}] Picked up job {job.id}")
                    # 2. Process it
                    processor = JobProcessor(job, self.storage, self.serializer)
                    processor.process()
                    print(f"[{self.worker_id}] Finished processing job {job.id}")
                

            except KeyboardInterrupt:
                print(f"[{self.worker_id}] Shutdown requested...")
                self._shutdown_requested = True
            except Exception as e:
                print(f"[{self.worker_id}] Unhandled exception in worker loop: {e}")
                time.sleep(5) # Cooldown period after a major failure
        
        print(f"[{self.worker_id}] Worker has stopped.")
