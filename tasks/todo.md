# Checklist de execução

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
- [ ] Validar renderização no GitHub e no PyPI.

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
- [ ] Confirmar que upload de Pages volta a funcionar.

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
- [ ] Testar patch, minor e major em branches descartáveis.
- [x] Testar ausência de release (dry-run informa corretamente que `v1.1.1` é atual).
- [ ] Testar falha por divergência de versão.
- [ ] Testar tag válida sem publicar uma versão real.

## Fechamento

- [x] Executar a validação local descrita no plano.
- [x] Executar a matriz final no GitHub Actions.
- [ ] Revisar permissões e pinagem das GitHub Actions.
- [ ] Confirmar badges PyPI, Python, CI e Codecov.
- [ ] Confirmar site público do Zensical.
- [x] Executar dry-run documentado do próximo release.
- [ ] Atualizar este checklist com resultados e links dos PRs.
