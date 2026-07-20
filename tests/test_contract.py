import os
import signal
import subprocess
import sys
import tempfile
import time

import pytest

from simpleq import Empty, LeaseExpired, SimpleQueue


class TestContract:
    def test_two_queues_same_db(self, tmp_path):
        """Duas filas com nomes diferentes no mesmo banco."""
        path = tmp_path / "queues"
        q1 = SimpleQueue(str(path), name="foo", lease_seconds=5.0)
        q2 = SimpleQueue(str(path), name="bar", lease_seconds=5.0)

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
        q1 = SimpleQueue(str(path), lease_seconds=0.3, max_retries=3)
        q1.put({"task": "one"})
        job = q1.get(block=False)
        q1.close()

        # Reabre sem ter dado ack/nack.
        q2 = SimpleQueue(str(path), lease_seconds=0.3, max_retries=3)
        time.sleep(0.4)
        job2 = q2.get(block=False)
        assert job2.data == {"task": "one"}
        assert job2.attempts == 1
        q2.ack(job2)
        q2.close()

    def test_crash_between_get_and_ack(self, tmp_path):
        """Processo morre imediatamente depois da reserva."""
        path = tmp_path / "crash"

        # Cria um processo que pega um job e morre sem ack.
        code = f"""
import sys
sys.path.insert(0, {repr(str(tmp_path))})
from simpleq import SimpleQueue

q = SimpleQueue({repr(str(path))}, lease_seconds=0.5, max_retries=3)
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
        q = SimpleQueue(str(path), lease_seconds=0.5, max_retries=3)
        time.sleep(0.6)
        job = q.get(block=False)
        assert job.data == {"task": "crash"}
        assert job.attempts == 1
        q.ack(job)
        q.close()

    def test_worker_a_expires_b_reserves_a_ack_rejected(self, tmp_path):
        """Worker A expira, B reserva, ACK atrasado de A é rejeitado."""
        path = tmp_path / "fencing"
        q = SimpleQueue(str(path), lease_seconds=0.3, max_retries=3)
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
        producer = SimpleQueue(str(path), lease_seconds=5.0)
        producer.put({"id": 1})
        producer.put({"id": 2})
        producer.put({"id": 3})

        stats = producer.stats()
        assert stats["ready"] == 3

        # Consumidor em outra instância.
        consumer = SimpleQueue(str(path), lease_seconds=5.0)
        job = consumer.get(block=False)
        consumer.ack(job)

        stats = consumer.stats()
        assert stats["ready"] == 2
        assert stats["acked"] == 1

        producer.close()
        consumer.close()
