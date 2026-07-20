import shutil
import time
from pathlib import Path

import pytest

from simpleq import Empty, SimpleQueue

DATA_DIR = Path(__file__).with_name("_test_data")


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "queue"
    q = SimpleQueue(str(path), lease_seconds=0.5, max_retries=2)
    yield q
    q.close()


class TestSimpleQueue:
    def test_put_and_get(self, queue):
        queue.put({"task": "one"})
        job = queue.get(block=False)
        assert job.data == {"task": "one"}
        assert job.attempts == 0
        queue.ack(job)
        assert queue.size() == 0

    def test_empty_raises_empty(self, queue):
        with pytest.raises(Empty):
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

    def test_failed_goes_to_dead_letter(self, queue):
        queue.put({"task": "bad"})
        job = queue.get(block=False)
        queue.fail(job)
        assert queue.size() == 0
        assert queue.failed_count() == 1

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
        assert queue.size() == 0
        assert queue.failed_count() == 1

    def test_reclaim_is_called_implicitly_on_get(self, queue):
        queue.put({"task": "auto"})
        job = queue.get(block=False)
        time.sleep(0.6)

        # get() chama reclaim_expired_leases() internamente.
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
        q1 = SimpleQueue(str(path), lease_seconds=10.0)
        q1.put({"task": "survive"})
        q1.close()

        q2 = SimpleQueue(str(path), lease_seconds=10.0)
        job = q2.get(block=False)
        assert job.data == {"task": "survive"}
        q2.ack(job)
        q2.close()
