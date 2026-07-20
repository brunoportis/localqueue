Para **fila persistente local, em uma única máquina**, eu começaria com:

## Recomendação principal: SQLite + ACK

Não subiria Kafka nem Redis só para isso.

Use uma fila baseada em SQLite com:

* `put()`
* `get()`
* `ack()`
* `nack()`
* retry
* estado `processing`
* timeout/lease para recuperar mensagens abandonadas por workers mortos

A opção pronta mais direta em Python é **`persist-queue`**, especialmente `SQLiteAckQueue`. Ela persiste os itens em disco, sobrevive a reinícios e oferece ACK/NACK. ([GitHub][1])

```bash
uv add persist-queue
```

```python
import persistqueue

queue = persistqueue.SQLiteAckQueue("./data/jobs")

queue.put({
    "type": "deploy",
    "app": "jarvis",
    "revision": "abc123",
})
```

Worker:

```python
import persistqueue

queue = persistqueue.SQLiteAckQueue("./data/jobs")

while True:
    job = queue.get()

    try:
        process(job)
    except TemporaryError:
        queue.nack(job)       # volta para a fila
    except Exception:
        queue.ack_failed(job) # dead-letter/falha definitiva
    else:
        queue.ack(job)
```

A biblioteca também tem FIFO, prioridade, filas assíncronas e serializers como JSON, MessagePack e CBOR. ([GitHub][1])

## Quando usar Huey

Se sua “mensagem” for realmente uma **chamada de função Python em background**, usaria **Huey + `SqliteHuey`**:

```bash
uv add huey
```

```python
from huey import SqliteHuey

huey = SqliteHuey(
    "local-jobs",
    filename="./data/jobs.db",
    fsync=True,
    strict_fifo=True,
    timeout=10,
    store_intermediate_errors=False,
)

@huey.task(
    retries=5,
    retry_delay=5,
    retry_backoff=2,
)
def deploy(app: str, revision: str) -> None:
    do_deploy(app, revision)
```

```bash
huey_consumer myapp.tasks.huey -w 4
```

Huey já entrega retries, backoff, prioridades, scheduling e workers. O backend SQLite usa WAL por padrão; `fsync=True` aumenta a durabilidade contra desligamento abrupto, embora custe desempenho. ([Huey][2])

A ressalva é importante: Huey é uma **task queue**, não um Kafka embutido. Em encerramento abrupto, uma tarefa já retirada da fila pode ser perdida; portanto, as tasks devem ser idempotentes ou você precisa tratar tarefas interrompidas. ([Huey][3])

## Quando SQLite deixa de ser suficiente

SQLite funciona muito bem quando tudo está no mesmo host, mas continua tendo **apenas um writer por vez**, mesmo em WAL. WAL permite que leitores e um writer trabalhem simultaneamente, mas não múltiplos writers reais. ([SQLite][4])

Se precisar de:

* vários grupos de consumidores independentes;
* replay completo do histórico;
* retenção por tempo/tamanho;
* consumidores entrando depois e lendo eventos antigos;
* comunicação entre várias máquinas;

então eu escolheria **NATS JetStream**. Ele é bem mais próximo de um “Kafka pequeno”: persiste streams, permite replay e possui consumidores duráveis, mas continua sendo um único binário leve. ([NATS Docs][5])

Se Redis já estiver instalado, use **Redis Streams + consumer groups + AOF**, não Redis Pub/Sub. Streams oferecem ACK, recuperação de mensagens pendentes, replay e múltiplos grupos; Pub/Sub pode perder a mensagem assim que o assinante desconecta. ([Redis][6])

## Minha escolha prática

Para algo local como um agent/daemon Python:

1. **Fila genérica e realmente baseada em mensagens:** `persist-queue.SQLiteAckQueue`.
2. **Executar funções Python com retries e scheduling:** `Huey + SqliteHuey`.
3. **Semântica parecida com Kafka:** NATS JetStream.
4. **Redis já existe no projeto:** Redis Streams com AOF.

Eu provavelmente começaria com **SQLiteAckQueue**, mantendo os handlers idempotentes e adicionando um `job_id` para deduplicação.

Um detalhe atual: ao usar SQLite em WAL com múltiplos processos, confira:

```python
import sqlite3

print(sqlite3.sqlite_version)
```

O SQLite corrigiu em 2026 um raro bug de concorrência no reset do WAL. Prefira `3.51.3+` ou versões com backport, como `3.50.7` e `3.44.6`. ([SQLite][4])

[1]: https://github.com/peter-wangxu/persist-queue "GitHub - peter-wangxu/persist-queue: A thread-safe disk based persistent queue in Python · GitHub"
[2]: https://huey.readthedocs.io/en/latest/api.html "Huey’s API — huey 3.2.1 documentation"
[3]: https://huey.readthedocs.io/en/latest/guide.html "Guide — huey 3.2.1 documentation"
[4]: https://sqlite.org/wal.html "Write-Ahead Logging"
[5]: https://docs.nats.io/nats-concepts/jetstream?utm_source=chatgpt.com "JetStream - NATS Docs"
[6]: https://redis.io/docs/latest/develop/use-cases/streaming/?utm_source=chatgpt.com "Redis streaming | Docs"

