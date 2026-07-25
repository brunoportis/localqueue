# Checklist: ingestion com identity e capacity

## Contrato

- [ ] Documentar que backpressure pode esperar indefinidamente.
- [ ] Adicionar exemplo com `asyncio.timeout()`.
- [ ] Explicar que timeout aguarda commit nativo em voo terminar.
- [ ] Adicionar teste documental do contrato.

## Benchmark

- [ ] Separar preload e janela medida.
- [ ] Adicionar bancos de 100 mil, 1 milhão e 6 milhões.
- [ ] Adicionar identidade nova, duplicada e conflito de fingerprint.
- [ ] Preservar capacity on/off, fan-out 1/5 e batches 100/1.000/10.000.
- [ ] Manter baseline de `dispatch()` repetido.
- [ ] Medir p50/p95/max da transação e tempo sob writer lock.
- [ ] Registrar RSS, memória Python, DB/WAL/SHM, ambiente e commit.
- [ ] Executar smoke de 100 mil.
- [ ] Aprovar orçamento operacional antes da matriz release.
- [ ] Executar matriz de 1 milhão e 6 milhões.

## Decisão

- [ ] Comparar resultados com o orçamento aprovado.
- [ ] Registrar GO para point queries ou NO-GO com otimização obrigatória.

## Otimização condicional

- [ ] Prototipar temp table set-based por queue.
- [ ] Avaliar CTE `VALUES` em chunks como alternativa.
- [ ] Cobrir limite de parâmetros SQLite.
- [ ] Criar testes diferenciais entre point queries e set-based.
- [ ] Validar atomicidade e crash failpoints.
- [ ] Rodar benchmark A/B.
- [ ] Manter a variante nova somente com ganho material comprovado.

## Fechamento

- [ ] Atualizar documentação com recomendação baseada nos dados.
- [ ] Atualizar PR body e anexar relatório final.
- [ ] Rodar Python, Rust, lint, tipos, docs e crash-test.
- [ ] Revisão humana antes do merge.
