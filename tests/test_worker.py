import inspect
import time

import pytest

from localqueue import LeaseExpired, SimpleQueue, Worker


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

    def test_worker_survives_heartbeat_lease_loss(self, tmp_path):
        """O worker não morre quando o heartbeat perde o lease."""
        import threading
        import time
        from unittest.mock import patch

        path = tmp_path / "heartbeat_survive"
        q = SimpleQueue(str(path), lease_seconds=5.0, max_retries=3)

        def handler(job):
            time.sleep(0.2)
            return "done"

        q.put({"id": 1})
        worker = Worker(q, handler, heartbeat_interval=0.05)

        def failing_extend(job, seconds):
            raise LeaseExpired("simulated lease loss")

        error = None

        def run_worker():
            nonlocal error
            try:
                worker.run_once()
            except Exception as exc:
                error = exc

        with patch.object(q, "extend_lease", side_effect=failing_extend):
            worker_thread = threading.Thread(target=run_worker)
            worker_thread.start()
            worker_thread.join(timeout=5)

        # O worker deve ter terminado sem exceção.
        assert error is None
        assert not worker_thread.is_alive()

        q.close()

    def test_worker_survives_ack_after_natural_lease_expiry(self, tmp_path):
        q = SimpleQueue(str(tmp_path / "expired_ack"), lease_seconds=0.03)
        q.put({"id": 1})

        def handler(job):
            time.sleep(0.06)
            return "done"

        worker = Worker(q, handler)

        assert worker.run_once() is True
        assert q.stats()["acked"] == 0
        q.close()

    def test_worker_survives_nack_after_natural_lease_expiry(self, tmp_path):
        q = SimpleQueue(str(tmp_path / "expired_nack"), lease_seconds=0.03)
        q.put({"id": 1})

        def handler(job):
            time.sleep(0.06)
            raise TransientError("late failure")

        worker = Worker(q, handler)

        assert worker.run_once() is True
        assert q.stats()["processing"] == 1
        q.close()

    def test_worker_survives_fail_after_natural_lease_expiry(self, tmp_path):
        q = SimpleQueue(str(tmp_path / "expired_fail"), lease_seconds=0.03)
        q.put({"id": 1})

        def handler(job):
            time.sleep(0.06)
            raise PermanentError("late permanent failure")

        worker = Worker(q, handler, permanent_errors=(PermanentError,))

        assert worker.run_once() is True
        assert q.stats()["failed"] == 0
        q.close()

    def test_run_once_relies_on_get_for_lease_reclaim(self, queue, monkeypatch):
        queue.put({"id": 1})

        def unexpected_reclaim():
            raise AssertionError("worker must not reclaim leases explicitly")

        monkeypatch.setattr(queue, "reclaim_expired_leases", unexpected_reclaim)

        assert Worker(queue, lambda job: None).run_once() is True

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"poll_interval": 0}, "poll_interval"),
            ({"poll_interval": -1}, "poll_interval"),
            ({"heartbeat_interval": 0}, "heartbeat_interval"),
            ({"heartbeat_interval": -1}, "heartbeat_interval"),
        ],
    )
    def test_worker_rejects_non_positive_intervals(self, queue, kwargs, message):
        with pytest.raises(ValueError, match=message):
            Worker(queue, lambda job: None, **kwargs)

    @pytest.mark.parametrize("heartbeat_interval", [10.0, 11.0])
    def test_worker_rejects_heartbeat_not_shorter_than_lease(
        self, queue, heartbeat_interval
    ):
        with pytest.raises(
            ValueError,
            match="heartbeat_interval.*smaller than.*lease",
        ):
            Worker(
                queue,
                lambda job: None,
                heartbeat_interval=heartbeat_interval,
            )

    def test_worker_no_longer_exposes_lease_reclaim_interval(self):
        parameters = inspect.signature(Worker).parameters

        assert "lease_reclaim_interval" not in parameters
