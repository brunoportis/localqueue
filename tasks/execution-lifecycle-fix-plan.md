# Plano: correções de consistência do ciclo de execução

## Objetivo

Corrigir as invariantes de execução durável do PR #90 sem introduzir a API
pública `EventBus.execute()`, worker automático ou tipos públicos de resultado.

## Ordem de implementação

1. Tornar `execution_open` uma comparação atômica de geração e fingerprint,
   recusando gerações de checkpoint sem runtime de execução.
2. Garantir que o cancelamento e a falha do heartbeat interrompam a ingestão
   reivindicada e preservem a exceção causal.
3. Revalidar o snapshot após finalização otimista e reconciliar a primitiva
   legada de conclusão da fonte com a tabela de runtime.
4. Cobrir as corridas e cenários de recuperação exigidos antes da validação
   completa.

## Invariantes

- Uma geração não nula pertence a exatamente um runtime de execução existente.
- Geração e fingerprint observados por `execution_open` vêm da mesma
  transação `IMMEDIATE`.
- Perda de lease do heartbeat nunca é descartada enquanto a ingestão está ativa.
- `wait()` só retorna snapshot finalizado, sem deliveries READY ou PROCESSING.
- `source_completed` e `source_completed_at` permanecem consistentes.

## Verificação por fatia

- Teste de regressão focado primeiro (falha antes da correção).
- `pytest` dos módulos de lifecycle/ingestão relacionados.
- `cargo test` e os checks de lint/tipo pertinentes após alterar Rust.

## Riscos

- Corridas SQLite exigem sincronização determinística nos testes, não `sleep`.
- Cancelamento de `asyncio.to_thread` precisa aguardar commit em voo antes de
  liberar lease.
- Não alterar APIs públicas ou iniciar consumidores automaticamente.
