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


def producer_process(path, name, num_jobs, result_queue):
    """Produtor em processo separado usando put_many com job_ids."""
    from localqueue import EnqueueItem

    q = SimpleQueue(path, name=name, lease_seconds=2.0, max_retries=1)
    ids = q.put_many(
        [EnqueueItem(data={"id": i}, job_id=f"job-{i}") for i in range(num_jobs)]
    )
    result_queue.put(ids)
    q.close()


class TestConcurrency:
    def test_empty_get_does_not_wait_for_native_writer(self, tmp_path):
        path = tmp_path / "idle-reader"
        writer = SimpleQueue(str(path), name="writer")
        idle = SimpleQueue(str(path), name="idle")
        wal_path = path / "localqueue.db-wal"
        initial_wal_size = wal_path.stat().st_size
        errors = []

        def write_large_batch():
            try:
                writer._native.put_many([b"{}"] * 500_000, None)
            except BaseException as exc:  # pragma: no cover - diagnóstico
                errors.append(exc)

        write_thread = threading.Thread(target=write_large_batch)
        write_thread.start()
        try:
            deadline = time.monotonic() + 5.0
            while (
                write_thread.is_alive()
                and wal_path.stat().st_size <= initial_wal_size
                and time.monotonic() < deadline
            ):
                time.sleep(0.001)

            assert wal_path.stat().st_size > initial_wal_size
            started = time.monotonic()
            with pytest.raises(Empty):
                idle.get(block=False)
            elapsed = time.monotonic() - started
        finally:
            write_thread.join(timeout=10)
            writer.close()
            idle.close()

        assert errors == []
        assert not write_thread.is_alive()
        assert elapsed < 0.2

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

    def test_concurrent_put_many_deduplicates_across_processes(self, tmp_path):
        """Dois processos produzindo em lote com os mesmos job_ids."""
        path = tmp_path / "multiproc-batch"
        q = SimpleQueue(str(path), lease_seconds=2.0, max_retries=1)
        q.close()

        num_jobs = 50
        result_queue = multiprocessing.Queue()
        processes = [
            multiprocessing.Process(
                target=producer_process,
                args=(str(path), "batch", num_jobs, result_queue),
            )
            for _ in range(2)
        ]
        for p in processes:
            p.start()
        for p in processes:
            p.join(timeout=30)
            assert p.exitcode == 0

        all_ids = []
        while not result_queue.empty():
            all_ids.extend(result_queue.get())

        # Cada job_id existe uma única vez; os dois processos veem os mesmos ids.
        assert len(all_ids) == 2 * num_jobs
        assert len(set(all_ids)) == num_jobs

        q = SimpleQueue(str(path), name="batch", lease_seconds=2.0, max_retries=1)
        assert q.stats()["ready"] == num_jobs
        q.close()
