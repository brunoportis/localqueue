# simpleq

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
simpleq/
├── src/               # Rust: schema, storage, queue, error, lib (pyo3)
├── python/simpleq/    # Python: SimpleQueue, Job, Worker, exceções
├── Cargo.toml
└── pyproject.toml     # build via maturin
```

Toda a máquina de estados vive em uma única tabela SQLite (`messages`), com
transações `BEGIN IMMEDIATE` para atomicidade. Não há camadas auxiliares de
estado: cada operação (`put`, `get`, `ack`, `nack`, `fail`) é uma única
transação.

## Uso rápido

```python
from simpleq import SimpleQueue

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
from simpleq import SimpleQueue, Worker

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
exemplo, se um worker morrer logo após dar `ack` mas antes de persistir).
Recomenda-se que os handlers sejam idempotentes — use `job_id` para
deduplicação quando necessário.

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
