"""Exemplo básico de produtor e consumidor com localqueue."""

from localqueue import DeliveryPolicy, SimpleQueue

DATA_DIR = "./data/example"


def producer():
    with SimpleQueue(
        DATA_DIR, delivery=DeliveryPolicy(lease_seconds=30, max_retries=3)
    ) as q:
        q.put({"type": "deploy", "app": "jarvis", "revision": "abc123"})
        print("Job enfileirado.")


def consumer():
    with SimpleQueue(
        DATA_DIR, delivery=DeliveryPolicy(lease_seconds=30, max_retries=3)
    ) as q:
        job = q.get(block=True, timeout=5)
        print(f"Processando: {job.data} (tentativa {job.attempts})")
        # Simula processamento bem-sucedido.
        q.ack(job)
        print("Job confirmado.")


if __name__ == "__main__":
    producer()
    consumer()
