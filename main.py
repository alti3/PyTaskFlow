# main.py
import time
from pytaskflow.client import BackgroundJobClient
from pytaskflow.config import configure
from pytaskflow.storage.memory_storage import MemoryStorage
from pytaskflow.server.worker import Worker


def sample_task(x, y):
    print(f"Executing sample_task with args: {x}, {y}")
    return x + y


if __name__ == "__main__":
    # 1. Configure PyTaskFlow
    storage = MemoryStorage()
    configure(storage)

    # 2. Create a client
    client = BackgroundJobClient(storage)

    # 3. Enqueue a job
    job_id = client.enqueue(sample_task, 1, 2)
    print(f"Enqueued job {job_id} for sample_task(1, 2)")

    # 4. Start a worker in a separate thread (for demonstration)
    import threading

    worker = Worker(storage, queues=["default"])
    worker_thread = threading.Thread(target=worker.run, daemon=True)
    worker_thread.start()

    # 5. Wait and check job status
    time.sleep(5)  # Give the worker time to process

    job_data = storage.get_job_data(job_id)
    print(f"\nJob data after execution: {job_data}")

    # Stop the worker gracefully
    worker._shutdown_requested = True
    worker_thread.join(timeout=5)
    print("\nDemonstration finished.")
