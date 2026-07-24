import time

import pytest
from localqueue import DeliveryPolicy, SimpleQueue


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "maintenance"
    q = SimpleQueue(
        str(path), delivery=DeliveryPolicy(lease_seconds=0.3, max_retries=1)
    )
    yield q
    q.close()


class TestMaintenance:
    def test_purge_acked(self, queue):
        queue.put({"task": "one"})
        job = queue.get(block=False)
        queue.ack(job)

        assert queue.stats()["acked"] == 1

        time.sleep(0.1)

        # Purge com idade pequena remove mensagens antigas.
        removed = queue.purge(0.05)
        assert removed == 1
        assert queue.stats()["acked"] == 0

    def test_purge_respects_age(self, queue):
        queue.put({"task": "one"})
        job = queue.get(block=False)
        queue.ack(job)

        # Purge com idade grande não remove nada.
        removed = queue.purge(3600)
        assert removed == 0
        assert queue.stats()["acked"] == 1

    def test_purge_failed(self, queue):
        queue.put({"task": "bad"})
        job = queue.get(block=False)
        queue.fail(job, "error")

        assert queue.stats()["failed"] == 1

        time.sleep(0.1)

        # Por padrão, purge não remove failed.
        removed = queue.purge(0.05)
        assert removed == 0
        assert queue.stats()["failed"] == 1

        # Com include_failed=True, remove.
        removed = queue.purge(0.05, include_failed=True)
        assert removed == 1
        assert queue.stats()["failed"] == 0

    def test_list_failed(self, queue):
        queue.put({"task": "bad1"})
        queue.put({"task": "bad2"})

        job1 = queue.get(block=False)
        queue.fail(job1, "error1")

        job2 = queue.get(block=False)
        queue.fail(job2, "error2")

        failed = queue.list_failed()
        assert len(failed) == 2
        assert failed[0]["data"] == {"task": "bad1"}
        assert failed[0]["last_error"] == "error1"
        assert failed[1]["data"] == {"task": "bad2"}
        assert failed[1]["last_error"] == "error2"

    def test_retry_failed(self, queue):
        queue.put({"task": "bad"})
        job = queue.get(block=False)
        queue.fail(job, "error")

        assert queue.stats()["failed"] == 1
        assert queue.stats()["ready"] == 0

        failed = queue.list_failed()
        message_id = failed[0]["id"]

        queue.retry_failed(message_id)

        assert queue.stats()["failed"] == 0
        assert queue.stats()["ready"] == 1

        # A mensagem pode ser processada novamente.
        job2 = queue.get(block=False)
        assert job2.data == {"task": "bad"}
        queue.ack(job2)

    def test_retry_failed_not_found(self, queue):
        from localqueue import LocalQueueError

        with pytest.raises(LocalQueueError):
            queue.retry_failed(999)

    def test_vacuum(self, queue):
        queue.put({"task": "one"})
        job = queue.get(block=False)
        queue.ack(job)

        # Vacuum não deve falhar.
        queue.vacuum()

        # A fila continua funcionando.
        queue.put({"task": "two"})
        job2 = queue.get(block=False)
        assert job2.data == {"task": "two"}
        queue.ack(job2)

    def test_purge_rejects_negative_age(self, queue):
        with pytest.raises(ValueError, match="older_than"):
            queue.purge(-1)

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"limit": -1}, "limit"),
            ({"offset": -1}, "offset"),
        ],
    )
    def test_list_failed_rejects_negative_pagination(self, queue, kwargs, message):
        with pytest.raises(ValueError, match=message):
            queue.list_failed(**kwargs)
