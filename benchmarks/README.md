# Benchmarks

Instale a dependência opcional e execute o benchmark fora da suíte normal de
testes:

```bash
uv pip install -e '.[benchmark]'
python benchmarks/queue_bench.py --backend both --operation roundtrip
```

Operações disponíveis:

- `write`: somente `put()`;
- `read_ack`: fila previamente carregada, medindo `get() + ack()`;
- `roundtrip`: `put() + get() + ack()` por item.

O benchmark usa payloads equivalentes serializados com pickle nos dois
backends. Ele mede o caminho single-processo; concorrência e multiprocessos
devem ser executados em um cenário separado, porque `persist-queue` documenta
principalmente segurança em threads.
