import pytest

from simpleq import LeaseExpired, SimpleQueue, Worker


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

    def test_heartbeat_failure_does_not_ack_running_handler(self, tmp_path):
        """Se o heartbeat falhar, o job não deve ser confirmado."""
        import threading
        import time
        from unittest.mock import patch

        path = tmp_path / "heartbeat"
        q = SimpleQueue(str(path), lease_seconds=5.0, max_retries=3)

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def handler(job):
            handler_started.set()
            handler_continue.wait(timeout=5)
            return "done"

        q.put({"id": 1})
        worker = Worker(q, handler, heartbeat_interval=0.1)

        # Força extend_lease a falhar após o handler começar.
        original_extend = q.extend_lease

        def failing_extend(job, seconds):
            raise LeaseExpired("simulated lease loss")

        with patch.object(q, "extend_lease", side_effect=failing_extend):
            # Roda o worker em uma thread para poder controlar o handler.
            worker_thread = threading.Thread(target=worker.run_once)
            worker_thread.start()

            # Espera o handler começar.
            assert handler_started.wait(timeout=2)

            # Espera o heartbeat falhar algumas vezes.
            time.sleep(0.3)

            # Libera o handler.
            handler_continue.set()
            worker_thread.join(timeout=5)

        # O job não deve ter sido confirmado (deve estar em ready).
        stats = q.stats()
        assert stats["acked"] == 0

        q.close()
