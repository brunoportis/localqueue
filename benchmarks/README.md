# Benchmarks

Instale a dependência opcional e execute o benchmark fora da suíte normal de
testes:

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

O benchmark usa payloads equivalentes serializados com pickle nos dois
backends. Ele mede o caminho single-processo; concorrência e multiprocessos
devem ser executados em um cenário separado, porque `persist-queue` documenta
principalmente segurança em threads.
