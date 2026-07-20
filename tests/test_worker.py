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

        def handler(data):
            processed.append(data)
            return "ok"

        queue.put({"id": 1})
        worker = Worker(queue, handler)
        worker.run_once()

        assert len(processed) == 1
        assert queue.acked_count() == 1
        assert queue.size() == 0

    def test_worker_nacks_transient_error(self, queue):
        attempts = []

        def handler(data):
            attempts.append(data)
            raise TransientError("ops")

        queue.put({"id": 1})
        worker = Worker(queue, handler)
        worker.run_once()

        assert len(attempts) == 1
        assert queue.acked_count() == 0
        assert queue.failed_count() == 0
        assert queue.size() == 1

    def test_worker_fails_permanent_error(self, queue):
        def handler(_):
            raise PermanentError("nope")

        queue.put({"id": 1})
        worker = Worker(queue, handler, permanent_errors=(PermanentError,))
        worker.run_once()

        assert queue.acked_count() == 0
        assert queue.failed_count() == 1
        assert queue.size() == 0

    def test_worker_returns_false_when_empty(self, queue):
        worker = Worker(queue, lambda x: x)
        assert worker.run_once() is False
