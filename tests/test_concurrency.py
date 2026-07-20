import threading
import time

import pytest

from simpleq import SimpleQueue


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "concurrent"
    q = SimpleQueue(
        str(path), lease_seconds=2.0, max_retries=1, multithreading=True
    )
    yield q
    q.close()


class TestConcurrency:
    def test_multiple_workers_do_not_duplicate_jobs(self, queue):
        """Vários workers competindo pela fila não processam o mesmo job."""
        num_jobs = 50
        for i in range(num_jobs):
            queue.put({"id": i})

        processed = []
        processed_lock = threading.Lock()

        def worker():
            while True:
                with processed_lock:
                    if len(processed) >= num_jobs:
                        break

                try:
                    job = queue.get(block=False)
                except Exception:
                    time.sleep(0.01)
                    continue

                with processed_lock:
                    processed.append(job.data["id"])
                queue.ack(job)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert len(processed) == num_jobs
        assert len(set(processed)) == num_jobs
        assert queue.acked_count() == num_jobs
        assert queue.size() == 0
        assert queue.unack_count() == 0
