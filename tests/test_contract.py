import os
import subprocess
import sys
import tempfile
import time
import typing

import pytest
from localqueue import (
    DeliveryPolicy,
    Empty,
    Job,
    LeaseExpired,
    LocalQueueError,
    SimpleQueue,
)


def _put_job_with_same_id(path: str, result_queue) -> None:
    q = SimpleQueue(path, delivery=DeliveryPolicy(lease_seconds=5.0))
    job_id = q.put({"task": "x"}, job_id="job-123")
    result_queue.put(job_id)
    q.close()


class TestContract:
    def test_two_queues_same_db(self, tmp_path):
        """Duas filas com nomes diferentes no mesmo banco."""
        path = tmp_path / "queues"
        q1 = SimpleQueue(
            str(path), name="foo", delivery=DeliveryPolicy(lease_seconds=5.0)
        )
        q2 = SimpleQueue(
            str(path), name="bar", delivery=DeliveryPolicy(lease_seconds=5.0)
        )

        q1.put({"task": "foo"})
        q2.put({"task": "bar"})

        job1 = q1.get(block=False)
        job2 = q2.get(block=False)

        assert job1.data == {"task": "foo"}
        assert job2.data == {"task": "bar"}

        q1.ack(job1)
        q2.ack(job2)

        q1.close()
        q2.close()

    def test_reopen_with_processing_jobs(self, tmp_path):
        """Reabertura com jobs em processamento: não deve duplicar."""
        path = tmp_path / "reopen"
        q1 = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.2, max_retries=3)
        )
        try:
            q1.put({"task": "one"})
            first_job = q1.get(block=False)
        finally:
            q1.close()

        # Reabre sem ter dado ack/nack. O lease do claim recuperado é longo
        # para que o ACK abaixo não dependa da velocidade do runner.
        q2 = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=5.0, max_retries=3)
        )
        try:
            deadline = time.monotonic() + 2.0
            while True:
                try:
                    job2 = q2.get(block=False)
                    break
                except Empty:
                    if time.monotonic() >= deadline:
                        pytest.fail(
                            "o primeiro claim não expirou a tempo de ser reclamado"
                        )
                    time.sleep(0.02)

            assert job2.data == {"task": "one"}
            assert job2.attempts == 1
            assert job2.receipt != first_job.receipt

            # O receipt do claim expirado permanece inválido após o reclaim.
            with pytest.raises(LeaseExpired):
                q2.ack(first_job)

            q2.ack(job2)
            with pytest.raises(Empty):
                q2.get(block=False)
        finally:
            q2.close()

    def test_crash_between_get_and_ack(self, tmp_path):
        """Processo morre imediatamente depois da reserva."""
        path = tmp_path / "crash"

        # Cria um processo que pega um job e morre sem ack.
        code = f"""
import sys
sys.path.insert(0, {repr(str(tmp_path))})
from localqueue import DeliveryPolicy, SimpleQueue

q = SimpleQueue({repr(str(path))}, delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=3))
q.put({{"task": "crash"}})
job = q.get(block=False)
# Morre abruptamente sem ack.
import os
os._exit(1)
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            script = f.name

        try:
            result = subprocess.run(
                [sys.executable, script],
                capture_output=True,
                timeout=10,
            )
            assert result.returncode == 1
        finally:
            os.unlink(script)

        # Recupera o job abandonado.
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=3)
        )
        time.sleep(0.6)
        job = q.get(block=False)
        assert job.data == {"task": "crash"}
        assert job.attempts == 1
        q.ack(job)
        q.close()

    def test_worker_a_expires_b_reserves_a_ack_rejected(self, tmp_path):
        """Worker A expira, B reserva, ACK atrasado de A é rejeitado."""
        path = tmp_path / "fencing"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.3, max_retries=3)
        )
        q.put({"task": "fenced"})

        # Worker A pega o job.
        job_a = q.get(block=False)

        # Lease de A expira.
        time.sleep(0.4)

        # Worker B pega o job.
        job_b = q.get(block=False)
        assert job_b.receipt != job_a.receipt

        # ACK atrasado de A é rejeitado.
        with pytest.raises(LeaseExpired):
            q.ack(job_a)

        # ACK de B funciona.
        q.ack(job_b)
        q.close()

    def test_stats_multiprocess(self, tmp_path):
        """stats() correto com produtor e consumidor em processos separados."""
        path = tmp_path / "stats"

        # Produtor.
        producer = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
        producer.put({"id": 1})
        producer.put({"id": 2})
        producer.put({"id": 3})

        stats = producer.stats()
        assert stats["ready"] == 3

        # Consumidor em outra instância.
        consumer = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
        job = consumer.get(block=False)
        consumer.ack(job)

        stats = consumer.stats()
        assert stats["ready"] == 2
        assert stats["acked"] == 1

        producer.close()
        consumer.close()

    def test_ack_rejected_after_expiry_before_redelivery(self, tmp_path):
        """ACK atrasado antes de outro worker reservar deve ser rejeitado."""
        path = tmp_path / "expiry"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.3, max_retries=3)
        )
        q.put({"task": "stale"})

        job = q.get(block=False)
        time.sleep(0.4)

        with pytest.raises(LeaseExpired):
            q.ack(job)

        q.close()

    def test_nack_rejected_after_expiry_before_redelivery(self, tmp_path):
        """NACK atrasado antes de outro worker reservar deve ser rejeitado."""
        path = tmp_path / "expiry_nack"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.3, max_retries=3)
        )
        q.put({"task": "stale"})

        job = q.get(block=False)
        time.sleep(0.4)

        with pytest.raises(LeaseExpired):
            q.nack(job)

        q.close()

    def test_extend_lease_rejected_after_expiry(self, tmp_path):
        """extend_lease após expiração deve ser rejeitado."""
        path = tmp_path / "expiry_extend"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.3, max_retries=3)
        )
        q.put({"task": "stale"})

        job = q.get(block=False)
        time.sleep(0.4)

        with pytest.raises(LeaseExpired):
            job.extend_lease(10.0)

        q.close()

    def test_expired_job_does_not_exceed_max_attempts(self, tmp_path):
        """Job não é entregue mais vezes que max_attempts via reclaim."""
        path = tmp_path / "max_attempts"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.2, max_retries=2)
        )
        q.put({"task": "doomed"})

        # 3 entregas: 1 inicial + 2 retries.
        for _ in range(3):
            q.get(block=False)
            time.sleep(0.3)

        # Após expirar a terceira vez, deve ir para failed.
        with pytest.raises(Empty):
            q.get(block=False)
        assert q.stats()["failed"] == 1

        q.close()

    def test_concurrent_put_same_job_id_returns_same_id(self, tmp_path):
        """Dois processos fazendo put com mesmo job_id retornam o mesmo id."""
        import multiprocessing

        path = tmp_path / "dedup"

        result_queue = multiprocessing.Queue()
        processes = [
            multiprocessing.Process(
                target=_put_job_with_same_id,
                args=(str(path), result_queue),
            )
            for _ in range(2)
        ]
        for p in processes:
            p.start()
        for p in processes:
            p.join(timeout=10)

        results = [result_queue.get(timeout=5), result_queue.get(timeout=5)]
        assert results[0] == results[1]

        for p in processes:
            assert p.exitcode == 0

        q = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
        assert q.stats()["ready"] == 1
        q.close()

    def test_close_prevents_further_operations(self, tmp_path):
        """Após close(), operações devem levantar erro."""
        from localqueue import DeliveryPolicy, LocalQueueError

        path = tmp_path / "close"
        q = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
        q.put({"task": "x"})
        q.close()

        with pytest.raises(LocalQueueError):
            q.put({"task": "y"})

        with pytest.raises(LocalQueueError):
            q.get(block=False)

    def test_lease_expires_at_updates_on_extend(self, tmp_path):
        """lease_expires_at é atualizado após extend_lease."""
        path = tmp_path / "extend"
        q = SimpleQueue(
            str(path), delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=3)
        )
        q.put({"task": "x"})

        job = q.get(block=False)
        old_expiration = job.lease_expires_at

        job.extend_lease(10.0)
        assert job.lease_expires_at > old_expiration + 5

        q.ack(job)
        q.close()

    @pytest.mark.parametrize("transition", ["ack", "nack", "fail", "extend_lease"])
    def test_queue_cannot_transition_job_from_another_queue(self, tmp_path, transition):
        path = tmp_path / f"queue_isolation_{transition}"
        owner = SimpleQueue(
            str(path), name="emails", delivery=DeliveryPolicy(lease_seconds=5.0)
        )
        other = SimpleQueue(
            str(path), name="deploys", delivery=DeliveryPolicy(lease_seconds=5.0)
        )
        owner.put({"email": "x"})
        job = owner.get(block=False)

        with pytest.raises(LocalQueueError):
            if transition == "extend_lease":
                other.extend_lease(job, 5.0)
            else:
                getattr(other, transition)(job)

        assert owner.stats()["processing"] == 1
        owner.ack(job)
        owner.close()
        other.close()

    def test_get_nowait_return_annotation_is_job(self):
        hints = typing.get_type_hints(SimpleQueue.get_nowait)

        assert hints["return"] is Job
