# Checklist: correções de ciclo de execução do PR #90

- [ ] Open atômico: rejeitar geração sem runtime e validar fingerprint no Rust.
- [ ] Testar geração criada por `SourceDefinition.ingest()` e corrida reset/CAS.
- [ ] Propagar falha/perda de lease do heartbeat para a ingestão reivindicada.
- [ ] Revalidar finalização em `wait()` e reconciliar a primitiva legada.
- [ ] Adicionar cobertura de concorrência, fencing, rollback, retry, descendentes,
  cancelamento, restart/resume e multiprocess.
- [ ] Rodar verificações focadas e a suíte completa antes de atualizar o PR.
