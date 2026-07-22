# Benchmarks

O harness canônico fica no pacote instalado:

```bash
python -m localqueue.benchmark --profile multiprocess-ci --output multiprocess.json
python -m localqueue.benchmark render multiprocess.json --output multiprocess.md
```

Consulte `docs/benchmarks.md` para matriz, sampling, IDs, RSS, DB/WAL/SHM e o
cenário release de um milhão de linhas. Não há thresholds de performance.

Os comandos abaixo são wrappers comparativos legados:

```bash
uv pip install -e '.[benchmark]'
python benchmarks/queue_bench.py --backend both --operation roundtrip
```

Para comparar as duas políticas do localqueue, execute o mesmo cenário duas
vezes:

```bash
python benchmarks/queue_bench.py --durability normal --output normal.json
python benchmarks/queue_bench.py --durability full --output full.json
```

Cada resultado inclui `journal_mode`, `synchronous` e o nome da política
observada na conexão SQLite de cada backend. O `persist-queue` mantém sua
configuração padrão; no benchmark atual ela é registrada explicitamente para
que a diferença em relação a `NORMAL` não fique escondida.

Operações disponíveis:

- `write`: somente `put()`;
- `read_ack`: fila previamente carregada, medindo `get() + ack()`;
- `roundtrip`: `put() + get() + ack()` por item.
- `fanout`: `EventBus.dispatch()` para 1, 10 e 100 subscriptions declaradas
  em uma `BusTopology`, sem registrar handlers no processo produtor.

O benchmark usa payloads equivalentes serializados com pickle nos dois
backends. Ele mede o caminho single-processo; concorrência e multiprocessos
devem ser executados em um cenário separado, porque `persist-queue` documenta
principalmente segurança em threads.
