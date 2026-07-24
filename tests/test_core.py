import time

import pytest
from localqueue import DeliveryPolicy, Empty, SimpleQueue


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "queue"
    q = SimpleQueue(
        str(path), delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=2)
    )
    yield q
    q.close()


class TestSimpleQueue:
    def test_put_and_get(self, queue):
        queue.put({"task": "one"})
        job = queue.get(block=False)
        assert job.data == {"task": "one"}
        assert job.attempts == 0
        queue.ack(job)
        assert queue.stats()["ready"] == 0

    def test_empty_raises_empty(self, queue):
        with pytest.raises(Empty, match="queue is empty"):
            queue.get(block=False)

    def test_nack_returns_to_queue_and_increments_attempts(self, queue):
        queue.put({"task": "retry"})
        job = queue.get(block=False)
        assert job.attempts == 0
        queue.nack(job)

        job2 = queue.get(block=False)
        assert job2.data == {"task": "retry"}
        assert job2.attempts == 1
        queue.ack(job2)

    def test_nack_with_delay(self, queue):
        queue.put({"task": "delayed"})
        job = queue.get(block=False)
        queue.nack(job, delay=10.0)

        with pytest.raises(Empty):
            queue.get(block=False)

    @pytest.mark.parametrize("delay", [-0.001, -1.0])
    def test_nack_rejects_negative_delay(self, queue, delay):
        queue.put({"task": "invalid-delay"})
        job = queue.get(block=False)

        with pytest.raises(ValueError, match="delay.*non-negative"):
            queue.nack(job, delay=delay)

        queue.ack(job)

    def test_failed_goes_to_dead_letter(self, queue):
        queue.put({"task": "bad"})
        job = queue.get(block=False)
        queue.fail(job)
        assert queue.stats()["ready"] == 0
        assert queue.stats()["failed"] == 1

    def test_lease_expires_and_returns_to_queue(self, queue):
        queue.put({"task": "abandoned"})
        job = queue.get(block=False)
        assert job.data == {"task": "abandoned"}

        # Não dá ack/nack; simula worker morto esperando lease expirar.
        time.sleep(0.6)

        recovered = queue.reclaim_expired_leases()
        assert recovered == 1

        job2 = queue.get(block=False)
        assert job2.data == {"task": "abandoned"}
        assert job2.attempts == 1
        queue.ack(job2)

    def test_max_retries_moves_to_dead_letter(self, queue):
        queue.put({"task": "doomed"})

        for attempt in range(3):
            job = queue.get(block=False)
            assert job.data == {"task": "doomed"}
            assert job.attempts == attempt
            time.sleep(0.6)
            queue.reclaim_expired_leases()

        # Após max_retries=2, a próxima recuperação deve mandar para dead-letter.
        assert queue.stats()["ready"] == 0
        assert queue.stats()["failed"] == 1

    def test_nack_respects_max_retries(self, queue):
        queue.put({"task": "doomed"})

        job = queue.get(block=False)
        assert job.attempts == 0
        queue.nack(job)

        job = queue.get(block=False)
        assert job.attempts == 1
        queue.nack(job)

        job = queue.get(block=False)
        assert job.attempts == 2
        queue.nack(job)

        assert queue.stats()["ready"] == 0
        assert queue.stats()["failed"] == 1

    def test_reclaim_is_called_implicitly_on_get(self, queue):
        queue.put({"task": "auto"})
        queue.get(block=False)
        time.sleep(0.6)

        # get() chama reclaim_expired_leases() internamente no backend.
        job2 = queue.get(block=False)
        assert job2.data == {"task": "auto"}
        assert job2.attempts == 1
        queue.ack(job2)

    def test_stats(self, queue):
        queue.put({"a": 1})
        queue.put({"b": 2})
        job = queue.get(block=False)
        queue.ack(job)

        stats = queue.stats()
        assert stats["ready"] == 1
        assert stats["processing"] == 0
        assert stats["acked"] == 1
        assert stats["failed"] == 0

    def test_persistence_across_reopen(self, tmp_path):
        path = tmp_path / "persist"
        q1 = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=10.0))
        q1.put({"task": "survive"})
        q1.close()

        q2 = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=10.0))
        job = q2.get(block=False)
        assert job.data == {"task": "survive"}
        q2.ack(job)
        q2.close()

    def test_deduplication_by_job_id(self, queue):
        queue.put({"task": "one"}, job_id="job-123")
        queue.put({"task": "two"}, job_id="job-123")
        queue.put({"task": "three"}, job_id="job-456")

        assert queue.stats()["ready"] == 2

    def test_extend_lease(self, queue):
        queue.put({"task": "long"})
        job = queue.get(block=False)

        # Estende o lease por mais 5 segundos.
        job.extend_lease(5.0)

        # Espera além do lease original mas dentro do novo.
        time.sleep(0.7)

        # O job ainda deve estar em processamento.
        stats = queue.stats()
        assert stats["processing"] == 1

        queue.ack(job)

    @pytest.mark.parametrize("seconds", [0.0, -1.0])
    def test_extend_lease_rejects_non_positive_duration(self, queue, seconds):
        queue.put({"task": "invalid-extension"})
        job = queue.get(block=False)

        with pytest.raises(ValueError, match="seconds.*positive"):
            job.extend_lease(seconds)

        queue.ack(job)

    def test_ack_after_lease_expired_raises(self, queue):
        queue.put({"task": "stale"})
        job = queue.get(block=False)

        # Espera o lease expirar.
        time.sleep(0.6)

        # Outro worker (ou o mesmo) recupera o job.
        job2 = queue.get(block=False)
        assert job2.receipt != job.receipt

        # ACK atrasado do primeiro worker deve ser rejeitado.
        from localqueue import LeaseExpired

        with pytest.raises(LeaseExpired, match="lease has expired"):
            queue.ack(job)

        # O segundo worker pode confirmar normalmente.
        queue.ack(job2)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"lease_seconds": 0}, "lease_seconds"),
            ({"lease_seconds": -1}, "lease_seconds"),
            ({"max_retries": -1}, "max_retries"),
        ],
    )
    def test_constructor_rejects_invalid_limits(self, tmp_path, kwargs, message):
        with pytest.raises(ValueError, match=message):
            DeliveryPolicy(**kwargs)
