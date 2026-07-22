# Checklist de execução

## Issue #19 — campanha operacional SQLite

- [ ] Infraestrutura de modelo, relatório, subprocessos e CLI
- [ ] Dez cenários obrigatórios e seleção individual
- [ ] Testes de contrato, falhas e execução fora da raiz
- [ ] Documentação interna e workflow Linux semanal
- [ ] Validação local completa, Actions e draft PR

Este checklist acompanha `tasks/plan.md`. Estado atual: **implementação e
publicação `v1.1.1` concluídas; aguardando propagação externa de Pages e
configuração do token Codecov**.

## Preparação

- [x] Plano revisado e aprovado pelo mantenedor.
- [x] Confirmar worktree limpo antes de cada PR.
- [ ] Criar branch dedicada para cada incremento.

## PR 1 — README e durabilidade

- [x] Escrever quickstart com produtor e worker em processos separados.
- [x] Executar literalmente os snippets.
- [x] Explicar persistência em `localqueue.db` e limites de `fsync`.
- [x] Corrigir cache da badge Python sem pin de versão do pacote.
- [x] Validar renderização no GitHub e no PyPI; corrigir a fence do Quick start.

## PR 2 — Qualidade e desempenho

- [x] Adicionar pytest-cov e baseline branch de 80%.
- [ ] Integrar Codecov e executar primeiro upload autenticado do `main`.
- [x] Adicionar badge Codecov.
- [x] Configurar Ruff check/format.
- [x] Corrigir achados objetivos do Ruff.
- [x] Configurar Pyrefly para `python/`.
- [x] Corrigir erros de tipo de produção sem ignores amplos.
- [x] Configurar pre-commit rápido.
- [x] Configurar Gitleaks local e no CI.
- [x] Adicionar `cargo-deny` e política de licenças/advisories/sources.
- [x] Adicionar cache Cargo à matriz Python.
- [x] Usar build debug na suíte, mantendo release para wheels.
- [x] Preservar timeouts de lease; não reduzir polling sem profiling adicional.
- [x] Rodar a suíte completa nas quatro combinações atuais.
- [x] Confirmar melhora mínima de 25% no Windows (2m12 → 1m37, ~27%).

## Cota de artifacts

- [x] Inventariar artifacts de `brunoportis/rokucontrol`.
- [x] Preservar últimos artifacts úteis e releases.
- [x] Excluir 21 artifacts antigos, preservando os dois mais recentes.
- [x] Registrar IDs, tamanhos e espaço recuperado (3,85 GiB).
- [ ] Confirmar que upload de Pages volta a funcionar após a recalculação de
  quota indicada pelo GitHub.

## PR 3 — Zensical e Pages

- [x] Adicionar dependência reproduzível do Zensical.
- [x] Criar `zensical.toml`.
- [x] Criar landing page em `docs/index.md`.
- [x] Criar getting started durável.
- [x] Organizar navegação das garantias e Event Bus.
- [x] Excluir conteúdo interno da navegação pública.
- [x] Manter CSS mínimo, usando o tema moderno do Zensical.
- [x] Validar build local limpo.
- [x] Adicionar validação de docs em PR.
- [x] Adicionar deploy no merge para `main`.
- [x] Configurar GitHub Pages para GitHub Actions.
- [ ] Validar desktop, mobile, teclado, links e URL publicada.

## PR 4 — SemVer e releases

- [x] Configurar Conventional Commits no Python Semantic Release.
- [x] Sincronizar versões de Python, Cargo e Cargo.lock.
- [x] Preservar changelog histórico usando atualização incremental.
- [x] Criar workflow manual com dry-run.
- [x] Impedir releases concorrentes.
- [x] Validar versão/tag antes dos builds de wheels.
- [x] Garantir que apenas `wheels.yml` publique GitHub Release e PyPI.
- [x] Reconciliar o histórico com a tag bootstrap ancestral `v1.1.1`, sem reescrever `v1.1.0`.
- [x] Testar patch, minor e major em clones descartáveis (`1.1.2`, `1.2.0`,
  `2.0.0`).
- [x] Testar ausência de release (dry-run informa corretamente que `v1.1.1` é atual).
- [ ] Testar falha por divergência de versão.
- [ ] Testar tag válida sem publicar uma versão real.

## Fechamento

- [x] Executar a validação local descrita no plano.
- [x] Executar a matriz final no GitHub Actions.
- [x] Revisar permissões e pinagem das GitHub Actions; todas as Actions usam
  SHA completo e cada workflow mantém apenas as permissões necessárias.
- [ ] Confirmar badges PyPI, Python, CI e Codecov (Codecov aguarda token e
  primeiro upload autenticado).
- [ ] Confirmar site público do Zensical.
- [x] Executar dry-run documentado do próximo release.
- [x] Atualizar este checklist com resultados e links finais.

## Evidências finais

- [CI completo](https://github.com/brunoportis/localqueue/actions/runs/29861997345):
  aprovado, incluindo wheel smoke test e cobertura de 89,55%.
- [Release v1.1.2](https://github.com/brunoportis/localqueue/releases/tag/v1.1.2):
  32 artefatos publicados no PyPI por Trusted Publishing.
- [Dry-run SemVer](https://github.com/brunoportis/localqueue/actions/runs/29849958301):
  confirmou corretamente que não havia release pendente após `v1.1.1`.
- GitHub Pages está configurado para o workflow oficial; o primeiro upload foi
  temporariamente recusado porque a quota de Actions ainda estava em
  recalculação após a limpeza de artifacts.
