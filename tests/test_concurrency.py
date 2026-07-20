import multiprocessing
import threading
import time

import pytest

from localqueue import Empty, SimpleQueue


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "concurrent"
    q = SimpleQueue(str(path), lease_seconds=2.0, max_retries=1)
    yield q
    q.close()


def worker_process(path, num_jobs, result_queue):
    """Worker em processo separado."""
    q = SimpleQueue(path, lease_seconds=2.0, max_retries=1)
    processed = []
    while len(processed) < num_jobs:
        try:
            job = q.get(block=False)
        except Empty:
            time.sleep(0.01)
            continue
        processed.append(job.data["id"])
        q.ack(job)
    result_queue.put(processed)
    q.close()


class TestConcurrency:
    def test_multiple_threads_do_not_duplicate_jobs(self, queue):
        """Vários workers na mesma thread não processam o mesmo job."""
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
                except Empty:
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
        assert queue.stats()["acked"] == num_jobs
        assert queue.stats()["ready"] == 0
        assert queue.stats()["processing"] == 0

    def test_multiple_processes_do_not_duplicate_jobs(self, tmp_path):
        """Dois processos reservando mil mensagens, sem entregas simultâneas."""
        path = tmp_path / "multiproc"
        q = SimpleQueue(str(path), lease_seconds=2.0, max_retries=1)
        num_jobs = 100
        for i in range(num_jobs):
            q.put({"id": i})
        q.close()

        result_queue = multiprocessing.Queue()
        processes = []
        jobs_per_process = num_jobs // 2

        for _ in range(2):
            p = multiprocessing.Process(
                target=worker_process,
                args=(str(path), jobs_per_process, result_queue),
            )
            p.start()
            processes.append(p)

        for p in processes:
            p.join(timeout=30)

        all_processed = []
        while not result_queue.empty():
            all_processed.extend(result_queue.get())

        assert len(all_processed) == num_jobs
        assert len(set(all_processed)) == num_jobs

        q = SimpleQueue(str(path), lease_seconds=2.0, max_retries=1)
        stats = q.stats()
        q.close()
        assert stats["acked"] == num_jobs
        assert stats["ready"] == 0
        assert stats["processing"] == 0
