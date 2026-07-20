import pytest

from simpleq import SimpleQueue, Worker


class PermanentError(Exception):
    pass


class TransientError(Exception):
    pass


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "worker_queue"
    q = SimpleQueue(str(path), lease_seconds=10.0, max_retries=3)
    yield q
    q.close()


class TestWorker:
    def test_worker_acks_success(self, queue):
        processed = []

        def handler(job):
            processed.append(job.data)
            return "ok"

        queue.put({"id": 1})
        worker = Worker(queue, handler)
        worker.run_once()

        assert len(processed) == 1
        assert queue.stats()["acked"] == 1
        assert queue.stats()["ready"] == 0

    def test_worker_nacks_transient_error(self, queue):
        attempts = []

        def handler(job):
            attempts.append(job.data)
            raise TransientError("ops")

        queue.put({"id": 1})
        worker = Worker(queue, handler)
        worker.run_once()

        assert len(attempts) == 1
        assert queue.stats()["acked"] == 0
        assert queue.stats()["failed"] == 0
        assert queue.stats()["ready"] == 1

    def test_worker_fails_permanent_error(self, queue):
        def handler(job):
            raise PermanentError("nope")

        queue.put({"id": 1})
        worker = Worker(queue, handler, permanent_errors=(PermanentError,))
        worker.run_once()

        assert queue.stats()["acked"] == 0
        assert queue.stats()["failed"] == 1
        assert queue.stats()["ready"] == 0

    def test_worker_returns_false_when_empty(self, queue):
        worker = Worker(queue, lambda job: job)
        assert worker.run_once() is False

    def test_worker_fails_after_max_retries(self, queue):
        def handler(job):
            raise TransientError("ops")

        queue.put({"id": 1})
        worker = Worker(queue, handler)

        for _ in range(4):
            worker.run_once()

        assert queue.stats()["ready"] == 0
        assert queue.stats()["failed"] == 1
