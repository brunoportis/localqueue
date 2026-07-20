# localqueue

Fila persistente local em SQLite com ACK, lease e retry.

O motor transacional é implementado em **Rust** (extensão nativa via pyo3/maturin),
garantindo atomicidade mesmo com múltiplos processos e threads. A facade em
**Python** mantém uma API simples e agradável.

## Características

- Persistência em disco via SQLite (sobrevive a reinícios).
- Semântica de ACK/NACK/falha com **receipt/fencing token**.
- Lease/timeout: jobs não confirmados dentro do prazo voltam para a fila.
- Retry com limite máximo; após isso vão para dead-letter.
- Deduplicação opcional por `job_id`.
- Extensão de lease (`job.extend_lease(...)`) e heartbeat no worker.
- Estatísticas consistentes mesmo em multiprocessos.
- Serialização JSON por padrão (legível e interoperável).
- Suporte a múltiplas filas no mesmo banco (`name="foo"`, `name="bar"`).

## Arquitetura

```text
localqueue/
├── src/               # Rust: schema, storage, queue, error, lib (pyo3)
├── python/localqueue/    # Python: SimpleQueue, Job, Worker, exceções
├── Cargo.toml
└── pyproject.toml     # build via maturin
```

Toda a máquina de estados vive em uma única tabela SQLite (`messages`), com
transações `BEGIN IMMEDIATE` para atomicidade. Não há camadas auxiliares de
estado: cada operação (`put`, `get`, `ack`, `nack`, `fail`) é uma única
transação.

## Uso rápido

```python
from localqueue import SimpleQueue

with SimpleQueue("./data", lease_seconds=30, max_retries=3) as q:
    q.put({"type": "deploy", "app": "jarvis", "revision": "abc123"})

    job = q.get()
    try:
        process(job.data)
    except Exception:
        q.nack(job)
    else:
        q.ack(job)
```

## Worker

```python
from localqueue import SimpleQueue, Worker

def deploy(job):
    print(f"Deploying {job.data['app']}@{job.data['revision']}")
    # Opcional: renova lease para jobs longos
    job.extend_lease(60)

q = SimpleQueue("./data")
worker = Worker(q, deploy, heartbeat_interval=10)
worker.run()
```

## Multithreading / multiprocessos

A fila é segura para uso com múltiplas threads e processos. O SQLite (em WAL)
serializa escritas, mas `BEGIN IMMEDIATE` e `busy_timeout` garantem que
operações concorrentes não corrompam o estado.

## Idempotência

Mesmo com fencing token, um job pode ser entregue mais de uma vez (por
exemplo, se o worker concluir um efeito externo e morrer antes de executar
`ack`). A fila oferece semântica at-least-once, não exactly-once.
Recomenda-se que os handlers sejam idempotentes — use `job_id` para
deduplicação quando necessário.

As garantias completas de durabilidade, leases, retries e fencing estão em
[`docs/guarantees.md`](docs/guarantees.md).

## API resumida

* ``put(data, job_id=None)`` – enfileira um item.
* ``get(block=True, timeout=None)`` – retira um item com lease.
* ``get_nowait()`` – variação não bloqueante.
* ``ack(job)`` – confirma processamento.
* ``nack(job, delay=0, last_error=None)`` – devolve à fila (erro transitório).
* ``fail(job, last_error=None)`` – envia para dead-letter.
* ``extend_lease(job, seconds)`` – renova o lease de um job.
* ``reclaim_expired_leases()`` – recupera leases expirados manualmente.
* ``stats()`` – retorna contagens de ready, processing, acked e failed.

## Manutenção

* ``purge(older_than, include_failed=False)`` – remove mensagens antigas.
* ``list_failed(limit=100, offset=0)`` – lista mensagens na dead-letter.
* ``retry_failed(message_id)`` – move mensagem failed de volta para ready.
* ``vacuum()`` – compacta todo o arquivo compartilhado ``localqueue.db``. Pode
  disputar o lock do SQLite com workers ativos, portanto prefira executá-lo em
  uma janela de manutenção.

## Desenvolvimento

```bash
uv sync --extra dev
maturin develop
pytest
```

## Build para distribuição

```bash
maturin build --release
```
