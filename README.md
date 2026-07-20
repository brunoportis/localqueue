# simpleq

Fila persistente local em SQLite com ACK, lease e retry.

Baseada na ideia documentada em `docs/internal/initial_idea.md`: usa
`persist-queue` (`SQLiteAckQueue`) como backend e adiciona uma camada de
*lease* para recuperar mensagens abandonadas por workers mortos.

## Características

- Persistência em disco via SQLite (sobrevive a reinícios).
- Semântica de ACK/NACK/falha.
- Lease/timeout: jobs não confirmados dentro do prazo voltam para a fila.
- Retry com limite máximo; após isso vão para dead-letter.
- Worker genérico incluso.
- Serialização JSON por padrão (legível e interoperável).

## Uso rápido

```python
from simpleq import SimpleQueue

with SimpleQueue("./data", lease_seconds=30, max_retries=3) as q:
    q.put({"type": "deploy", "app": "jarvis", "revision": "abc123"})

    job = q.get()
    try:
        process(job.data)
    except Exception:
        q.fail(job)
    else:
        q.ack(job)
```

## Worker

```python
from simpleq import SimpleQueue, Worker

def deploy(job):
    print(f"Deploying {job['app']}@{job['revision']}")

q = SimpleQueue("./data")
worker = Worker(q, deploy)
worker.run()  # loop contínuo
```

## Multithreading / multiprocessos

Para consumir a fila a partir de várias threads no mesmo processo, crie a
fila com ``multithreading=True``:

```python
q = SimpleQueue("./data", multithreading=True)
```

O SQLite (mesmo em WAL) permite apenas **um writer por vez**, então o
throughput de escritas é serial. Para workloads muito concorrentes, considere
NATS JetStream ou Redis Streams.

## Idempotência

Como qualquer fila com lease, um job pode ser entregue mais de uma vez (por
exemplo, se um worker morrer logo antes de dar ``ack``). Recomenda-se que os
handlers sejam idempotentes — adicione um ``job_id`` único ao payload quando
necessário.

## API resumida

* ``put(data)`` – enfileira um item.
* ``get(block=True, timeout=None)`` – retira um item com lease.
* ``get_nowait()`` – variação não bloqueante.
* ``ack(job)`` – confirma processamento.
* ``nack(job)`` – devolve à fila (erro transitório).
* ``fail(job)`` – envia para dead-letter.
* ``reclaim_expired_leases()`` – recupera leases expirados manualmente.
* ``stats()`` – retorna contagens de ready, processing, acked e failed.

## Desenvolvimento

```bash
uv sync --extra dev
pytest
```
