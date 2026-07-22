# Plano de evolução: documentação, qualidade, CI e releases

Status: **implementado em `main`; publicação `v1.1.1` concluída**

Data do baseline: 2026-07-21

## 1. Objetivo

Entregar uma evolução sustentável do projeto em quatro frentes:

1. demonstrar no quickstart que `localqueue` persiste jobs em SQLite e sobrevive
   ao encerramento do processo;
2. tornar qualidade, cobertura e segurança verificações reproduzíveis tanto
   localmente quanto no CI, reduzindo especialmente o tempo do job Windows;
3. transformar `docs/` em um site Zensical com landing page e publicação no
   GitHub Pages;
4. integrar SemVer ao processo de release sem duplicar a responsabilidade já
   exercida pelo workflow de wheels e publicação no PyPI.

## 2. Decisões confirmadas

- Cobertura: **Codecov**, mantendo um limite local bloqueante.
- GitHub Pages: liberar a cota removendo artifacts antigos do repositório
  `brunoportis/rokucontrol` e usar o workflow oficial baseado em Pages artifacts.
- Versionamento: release **manual e automatizado**, calculado a partir de
  Conventional Commits.
- Estratégia de entrega: PRs pequenos e ordenados, sem agrupar documentação,
  qualidade e release em uma única mudança difícil de revisar.

## 3. Baseline medido

| Sinal | Estado atual |
| --- | --- |
| Testes | 161 testes passando |
| Cobertura Python | 90% com branches (`570` statements, `46` ausentes) |
| Windows CI | aproximadamente 2m54 no último run observado |
| Build Rust no job Windows | aproximadamente 1m32 em `--release` |
| Testes no job Windows | aproximadamente 43s |
| Testes locais | aproximadamente 29,5s |
| Ruff padrão | 5 achados objetivos, todos corrigíveis |
| Ruff format | 6 arquivos fora do formato |
| Pyrefly padrão | 1 erro de produção e achados adicionais em testes |
| Vulture | redundante em alta confiança e ruidoso em baixa confiança |
| Python no PyPI | classificadores corretos para 3.10–3.14 |
| Badge Python | resposta antiga mantida pelo cache do Shields |

O limite inicial de cobertura será `80`, deixando margem para evolução e para
pequenas variações legítimas sem desproteger completamente a suíte. Aumentos
posteriores devem ser PRs deliberados.

## 4. Princípios de implementação

- O README publicado no PyPI não deve conter texto temporário sobre rollout ou
  uma versão específica da feature atual.
- A suíte completa continua rodando no Windows. A otimização não será feita por
  exclusão de testes ou redução de cobertura de plataforma.
- Esperas que representam garantias temporais reais serão preservadas; somente
  polling e timeouts de encerramento comprovadamente superdimensionados serão
  reduzidos.
- O limite de cobertura deve ser aplicado localmente pelo `pytest-cov`; Codecov
  fornece histórico, comparação e badge, mas não será a única barreira.
- Hooks de commit devem terminar rápido. Verificações caras pertencem ao CI.
- Ferramentas externas e GitHub Actions de terceiros devem ser versionadas de
  forma reproduzível; secrets nunca entram no repositório.
- O workflow SemVer decide versão e cria a tag. O workflow de wheels continua
  sendo o único responsável por construir e publicar distribuições.
- Cada PR precisa deixar o `main` publicável e reversível independentemente do
  PR seguinte.

## 5. Sequência de entrega

```text
PR 1: README e mensagem de durabilidade
  ↓
PR 2: cobertura, qualidade, segurança e desempenho do CI
  ↓
limpeza controlada de Actions artifacts
  ↓
PR 3: Zensical, landing page e GitHub Pages
  ↓
PR 4: SemVer e workflow manual de release
```

---

## 6. PR 1 — Quickstart durável e badges sustentáveis

### Escopo

Arquivos previstos:

- `README.md`
- possivelmente `docs/getting-started.md`, caso seja útil já preparar uma fonte
  expandida para o site sem duplicar todo o README

### Mudanças

1. Substituir o quickstart de processo único por dois programas executáveis:

   - `producer.py` abre `./data`, grava o job e encerra;
   - `worker.py`, executado depois, reabre o mesmo diretório, recebe o job e faz
     `ack()` ou `nack()`.

2. Inserir entre os exemplos uma frase explícita semelhante a:

   > At this point the producer may stop. The job is committed to
   > `./data/localqueue.db` and the worker can consume it in a later process.

3. Manter um exemplo curto o suficiente para a primeira leitura, mas mostrar:

   - o arquivo SQLite criado;
   - a separação temporal entre produção e consumo;
   - o ciclo `get()` → `ack()`/`nack()`;
   - que durabilidade após crash normal e durabilidade contra perda de energia
     possuem garantias diferentes, com link para `docs/guarantees.md`.

4. Preservar o exemplo separado de Event Bus, pois ele demonstra outra
   independência: topologia compartilhada, produtor sem handlers e worker com
   handlers locais.

5. Trocar a URL da badge Python por uma URL estável com
   `?cacheSeconds=300`. O parâmetro é uma política de cache, não uma versão do
   pacote, portanto não precisará ser alterado a cada release.

6. Adicionar a badge do Codecov somente depois do primeiro upload de cobertura
   bem-sucedido no PR 2, evitando uma badge `unknown` durante a transição.

### Testes e validação

- Copiar os dois blocos do quickstart para arquivos temporários e executá-los
  sequencialmente em processos distintos.
- Confirmar que o produtor encerra antes de o worker iniciar.
- Confirmar que `localqueue.db` existe após o produtor e que o worker recebe o
  payload esperado.
- Renderizar o README/PyPI description e verificar links e blocos de código.
- Consultar a badge Python com a nova query string e confirmar 3.10–3.14.

### Critérios de aceite

- Um leitor identifica a persistência sem precisar inferi-la da implementação.
- O exemplo funciona quando produtor e worker são comandos separados.
- Nenhuma instalação fica presa a `>=1.1.0` nem a qualquer versão futura.
- A badge Python deixa de usar a entrada de cache antiga do Shields.

### Rollback

Reverter somente o texto e a URL da badge. Nenhuma API ou storage é alterada.

---

## 7. PR 2 — Cobertura, qualidade, segurança e CI mais rápido

### 7.1 Dependências e configuração Python

Arquivos previstos:

- `pyproject.toml`
- `.pre-commit-config.yaml`
- `.gitignore`
- `.github/workflows/ci.yml`
- `codecov.yml`, somente se necessário para políticas explícitas
- arquivos Python/testes modificados por correções objetivas de lint e tipo

Adicionar ao ambiente de desenvolvimento, com faixas compatíveis e
reproduzíveis:

- `pytest-cov`;
- `ruff`;
- `pyrefly`;
- `pre-commit`.

Configurar:

- coverage com `branch = true`, fonte `localqueue`, relatório com linhas
  ausentes e `fail_under = 80`;
- Ruff com Python 3.10 como target mínimo;
- seleção inicial de regras de erro, imports, modernização compatível, bugs e
  simplificações de baixo risco;
- Pyrefly no modo padrão apenas sobre `python/` nesta primeira adoção.

Os achados em testes que dependem de detalhes opcionais da extensão nativa
serão corrigidos quando forem tipagem legítima. Não será adotado `strict` nem
serão adicionados `ignore` genéricos apenas para deixar o CI verde.

### 7.2 Política das ferramentas

#### Bloqueantes desde o primeiro PR

- `ruff check`;
- `ruff format --check`;
- `pyrefly check python`;
- `pytest` com branch coverage mínima de 80%;
- `cargo fmt --all --check`;
- `cargo clippy --locked --all-targets --all-features -- -D warnings`;
- Rust MSRV 1.83;
- `cargo deny check`;
- Gitleaks.

#### Pre-commit local, otimizado para latência

- whitespace/final newline;
- validação de TOML e YAML;
- Ruff check/format;
- `cargo fmt`;
- Gitleaks no conteúdo staged.

#### CI apenas

- Pyrefly;
- pytest completo e cobertura;
- Clippy;
- MSRV;
- `cargo-deny`;
- scan de secrets com histórico apropriado ao evento.

#### Não adotados agora

- **Vulture:** os resultados úteis já são cobertos por Ruff; abaixo de 80% de
  confiança surgem falsos positivos em modelos e handlers.
- **SonarCloud:** adicionaria outra integração SaaS e duplicaria lint, tipos e
  cobertura antes de haver uma necessidade concreta de análise centralizada.
- **cargo-machete:** as dependências Rust diretas atuais estão em uso e a
  ferramenta é heurística. Pode ser reconsiderada se o grafo crescer.

Essa decisão deverá ser documentada em `CONTRIBUTING.md` ou na seção de
desenvolvimento do README para impedir que a lista de ferramentas cresça sem
um problema mensurável.

### 7.3 Cobertura e Codecov

1. Gerar `coverage.xml` em um único job Linux canônico, evitando quatro uploads
   idênticos da matriz.
2. Fazer o pytest falhar localmente abaixo de 80%, independentemente da
   disponibilidade do Codecov.
3. Integrar a action oficial do Codecov usando `CODECOV_TOKEN` como repository
   secret. O token não será impresso nem disponibilizado a PRs de forks.
4. Habilitar a badge pública após o primeiro relatório do branch `main`.
5. Configurar política de comparação sem reduzir o limite local. Uma falha
   temporária do serviço Codecov não deve esconder uma falha de cobertura.
6. Evitar múltiplas fontes de verdade: `pyproject.toml` contém o mínimo; a
   configuração do Codecov apresenta tendência e diff.

Pré-requisito externo: conectar `brunoportis/localqueue` ao Codecov e salvar o
token como `CODECOV_TOKEN`. Se a conta estiver configurada para uploads públicos
sem token, manteremos a autenticação por token mesmo assim para proteger o
histórico do `main`.

### 7.4 Rust e segurança

Adicionar `deny.toml` com políticas explícitas para:

- advisories;
- licenças aceitas;
- crates duplicados ou banidos, inicialmente com exceções justificadas;
- fontes permitidas (`crates.io` e Git, apenas se algum dependency legítimo
  exigir).

Adicionar Gitleaks:

- no pre-commit para feedback imediato;
- no CI com saída redigida;
- sem baseline que silencie o histórico inteiro;
- qualquer falso positivo deve ser resolvido por allowlist mínima e comentada.

### 7.5 Redução do tempo no Windows

Mudanças propostas em ordem de menor risco:

1. adicionar cache Cargo por sistema operacional, versão Python, toolchain e
   hash de `Cargo.toml`/`Cargo.lock`;
2. usar `maturin develop --locked` em modo debug na matriz de testes;
3. manter builds `--release` no wheel smoke e no workflow de publicação;
4. eliminar a criação de um segundo ambiente quando `setup-python` já fornece
   isolamento suficiente, caso a medição confirme ganho sem perda de clareza;
5. separar instalação, build e pytest em steps para tornar os tempos visíveis;
6. medir os testes com `--durations`;
7. reduzir `idle_timeout=0.5` para um valor menor nos testes que apenas aguardam
   o consumer ficar ocioso;
8. não reduzir sleeps que comprovam expiração de lease até que o teste tenha
   margem explícita para runners lentos;
9. preferir sinais/eventos a sleeps quando o teste puder observar diretamente
   a condição esperada.

Não serão usados `pytest -n auto`, skips por plataforma ou retries globais para
mascarar flakiness.

### 7.6 Critérios de aceite

- 100% dos 161 testes atuais continuam cobertos pela matriz existente.
- Cobertura branch total permanece em pelo menos 80%.
- Um teste propositalmente sem cobertura suficiente demonstra que o gate falha.
- Ruff, Pyrefly, rustfmt, Clippy, MSRV, cargo-deny e Gitleaks passam localmente e
  no CI.
- O primeiro run sem cache continua correto.
- Um run Windows com cache quente melhora pelo menos 25% em relação ao baseline
  aproximado de 2m54. Se não melhorar, o PR não será considerado concluído sem
  novo profiling.
- O wheel smoke continua sendo feito em release e fora do checkout.
- A badge Codecov exibe o branch `main` e aponta para o relatório correto.

### 7.7 Rollback

- Reverter cache/debug build sem tocar na suíte.
- Se Codecov ficar indisponível, preservar o gate local e remover somente o
  upload/badge.
- Ferramentas novas entram em commits separados para permitir remover uma sem
  desfazer as demais.

---

## 8. Operação externa — liberar cota de Actions artifacts

Esta etapa ocorre antes do deploy do Pages e não altera código do
`localqueue`.

### Procedimento seguro

1. Inventariar artifacts de `brunoportis/rokucontrol` por nome, workflow, run,
   tamanho, criação e expiração.
2. Mostrar o inventário e o volume recuperável antes da exclusão.
3. Preservar:

   - artifacts do último run bem-sucedido de cada workflow ainda ativo;
   - qualquer artifact recente identificado como necessário;
   - releases e assets de releases, que não fazem parte desta limpeza.

4. Excluir primeiro artifacts expirados e os mais antigos.
5. Continuar apenas até existir margem segura para builds do `localqueue` e do
   GitHub Pages.
6. Registrar IDs e tamanhos excluídos no resumo da execução.
7. Confirmar que a cota deixou de bloquear `upload-pages-artifact`.

Artifacts excluídos não podem ser restaurados. Se os antigos não forem
suficientes, a operação deve parar antes de remover artifacts recentes e pedir
uma nova decisão.

---

## 9. PR 3 — Zensical, landing page e GitHub Pages

### Escopo

Arquivos previstos:

- `zensical.toml`
- `docs/index.md`
- `docs/getting-started.md`
- `docs/event-bus.md`
- `docs/guarantees.md`
- opcionalmente `docs/stylesheets/extra.css`
- `.github/workflows/docs.yml`
- `pyproject.toml` ou arquivo equivalente para dependência de docs
- `.gitignore` para `site/`

### Estrutura pública

```text
Home
├── Getting started
├── Queue guarantees
├── Event bus
├── API overview
└── Contributing / development
```

`docs/internal/initial_idea.md` permanece como contexto interno e não entra na
navegação pública sem revisão editorial.

### Landing page

A landing será técnica e orientada ao produto, com:

- hero: “durable local jobs without running another service”;
- exemplo visual produtor encerra → SQLite persiste → worker posterior consome;
- instalação em uma linha;
- diferenças claras para um `dict`, uma fila em memória e um broker de rede;
- cards curtos para durability, multiprocess safety, leases/retries/dead-letter
  e Event Bus;
- chamada para o quickstart e para as garantias;
- limites explícitos: comunicação local, at-least-once e ausência de replay de
  broker distribuído.

A página não tentará transformar o projeto em “mini Kafka”. A proposta central
continua sendo uma fila local durável e multiprocess-safe.

### Configuração Zensical

- `site_name = "localqueue"`;
- `site_url = "https://brunoportis.github.io/localqueue/"`;
- descrição, repository URL/name e links de edição;
- navegação explícita;
- recursos de busca, cópia de código e navegação apenas quando suportados pela
  versão fixada;
- custom CSS mínimo, focado em legibilidade e identidade, sem framework web
  paralelo.

A versão do Zensical será declarada em um local controlado e atualizável por
Dependabot. O build local documentado será:

```bash
zensical serve
zensical build --clean
```

### Workflow GitHub Pages

Criar `.github/workflows/docs.yml` com:

- validação de build em pull requests;
- deploy apenas em push no `main` ou acionamento manual;
- `contents: read`, `pages: write`, `id-token: write` apenas no job de deploy;
- concurrency própria de Pages;
- `zensical build --clean`;
- `actions/upload-pages-artifact` e `actions/deploy-pages`;
- environment `github-pages` com URL emitida pelo deploy.

Configurar o repositório para Pages → **GitHub Actions**. Não adicionar cache do
Zensical enquanto a documentação oficial desaconselhar essa prática.

### Validação

- Build limpo local e no CI.
- Navegação, links relativos, snippets e busca.
- Inspeção responsiva em viewport desktop e mobile.
- Acessibilidade básica: hierarquia de headings, foco visível, contraste,
  labels e navegação por teclado.
- Smoke test HTTP da URL publicada.
- README aponta para o site; links diretos ao GitHub continuam válidos quando
  úteis no PyPI.

### Critérios de aceite

- `zensical build --clean` passa sem warnings acionáveis.
- PRs validam docs sem publicar.
- Merge no `main` publica no GitHub Pages.
- A landing demonstra durabilidade antes de apresentar detalhes internos.
- O site funciona em mobile e desktop e não depende de JavaScript da aplicação.

### Rollback

Desativar o workflow e manter os Markdown em `docs/`; nenhum conteúdo depende
do Pages para continuar legível no GitHub.

---

## 10. PR 4 — SemVer manual automatizado

### Responsabilidades

```text
Conventional Commits
        │
        ▼
workflow manual de versão
        ├── calcula major/minor/patch
        ├── atualiza pyproject.toml
        ├── atualiza Cargo.toml e Cargo.lock
        ├── atualiza CHANGELOG.md
        ├── valida o tree versionado
        └── cria/pusha tag vX.Y.Z
                         │
                         ▼
                 wheels.yml existente
                         ├── build/smoke
                         ├── GitHub Release
                         └── PyPI trusted publishing
```

### Configuração

Adicionar Python Semantic Release com:

- parser Conventional Commits;
- tags `v{version}`;
- branch de release `main`;
- `version_toml` apontando para:

  - `pyproject.toml:project.version`;
  - `Cargo.toml:package.version`.

- changelog em modo de atualização, com marcador de inserção, preservando todo
  o histórico e a seção especial de migração existente;
- publicação de artifacts desabilitada no Semantic Release;
- GitHub Release desabilitada nele, porque `wheels.yml` já é o proprietário
  dessa etapa.

Semântica prevista:

| Commit | Bump |
| --- | --- |
| `fix:` | patch |
| `feat:` | minor |
| `BREAKING CHANGE:` ou `!` | major |
| `docs:`, `test:`, `ci:`, `chore:` | sem release por padrão |

### Workflow manual

Criar `.github/workflows/release.yml` com:

1. `workflow_dispatch` e modo `dry-run` disponível;
2. checkout completo (`fetch-depth: 0`) do `main`;
3. confirmação de que o SHA selecionado ainda é o HEAD de `main`;
4. cálculo e apresentação da próxima versão;
5. saída limpa quando não houver commit publicável;
6. atualização dos dois TOMLs;
7. sincronização de `Cargo.lock` via Cargo;
8. atualização incremental de `CHANGELOG.md`;
9. validação de igualdade entre:

   - versão Python;
   - versão Cargo;
   - pacote raiz no `Cargo.lock`;
   - tag candidata.

10. execução de checks suficientes no tree já versionado;
11. commit `chore(release): vX.Y.Z` e tag anotada;
12. push atômico do commit e da tag;
13. disparo natural do `wheels.yml` pela tag.

O workflow terá concurrency exclusiva para impedir dois releases simultâneos.
Permissões de escrita ficarão restritas ao job de release. Se a proteção do
`main` bloquear push pelo `GITHUB_TOKEN`, a alternativa será gerar um release
PR; não será usado PAT sem necessidade.

### Hardening do workflow de wheels

Antes de qualquer build de release:

- extrair `X.Y.Z` de `vX.Y.Z`;
- comparar a tag com `pyproject.toml`, `Cargo.toml` e `Cargo.lock`;
- rejeitar tags malformadas ou divergentes;
- manter trusted publishing do PyPI e o draft release já existente;
- definir retenção curta para artifacts de runs build-only, reduzindo nova
  pressão sobre a cota.

### Testes

- repositório fixture com commits `fix`, `feat` e breaking change;
- dry-run comprova patch/minor/major sem escrever no remoto;
- cenário sem commits publicáveis termina sem tag;
- divergência entre os três arquivos de versão falha antes do build;
- tag existente falha de forma idempotente, sem sobrescrever release;
- workflow de wheels continua aceitando uma tag válida e publicando somente
  depois que todas as plataformas terminam.

### Critérios de aceite

- Um operador inicia o workflow e vê a versão calculada antes da mutação.
- O release não exige editar manualmente números em arquivos.
- Python, Cargo, lockfile, changelog e tag permanecem sincronizados.
- Não existem dois publicadores concorrentes para GitHub Release/PyPI.
- A publicação continua usando PyPI trusted publishing.

### Rollback

- Antes do push: nenhum estado remoto foi alterado.
- Depois do commit/tag e antes do PyPI: parar o workflow de wheels e corrigir
  com uma nova versão; tags/release publicados não serão reescritos.
- Depois do PyPI: versões são imutáveis; qualquer correção usa um novo patch.

---

## 11. Estratégia de commits e revisão

Cada PR terá commits pequenos e convencionais. Exemplo:

```text
docs: demonstrate durable queue lifecycle
ci: enforce python coverage baseline
chore: add python and rust quality gates
ci: reduce cross-platform test setup time
docs: build documentation with zensical
ci: deploy documentation to github pages
ci: automate semantic version releases
```

Antes de mergear cada PR:

1. revisar o diff contra o escopo deste documento;
2. rodar os checks proporcionais ao PR;
3. revisar Actions de terceiros e permissões;
4. verificar que não há secrets, artifacts locais ou mudanças não relacionadas;
5. documentar a medição antes/depois quando houver objetivo de desempenho.

## 12. Validação final integrada

```bash
python -m pytest -ra \
  --cov=localqueue \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml \
  --cov-fail-under=80

ruff check .
ruff format --check .
pyrefly check python
pre-commit run --all-files

cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo check --locked
cargo deny check

zensical build --clean
```

Também serão executados:

- wheel smoke fora do checkout;
- build/test em Python 3.10 e 3.14 no Linux, 3.14 no macOS e Windows;
- teste real dos snippets duráveis do README;
- dry-run do release SemVer;
- smoke da URL do GitHub Pages;
- verificação das badges PyPI, Python, CI e Codecov.

## 13. Riscos principais

| Risco | Mitigação |
| --- | --- |
| Reduzir timeouts cria flakiness no Windows | Alterar somente polling/idle, medir runs frios e quentes, preservar margem de lease |
| Codecov indisponível bloqueia desenvolvimento | Cobertura local é a fonte de verdade; separar upload da execução do pytest |
| Gitleaks encontra segredo histórico | Redigir saída, rotacionar segredo real e usar allowlist somente para falso positivo comprovado |
| cargo-deny rejeita licença transitiva legítima | Exceção mínima, específica e documentada |
| Exclusão de artifacts remove algo necessário | Inventário prévio, preservar últimos runs e parar antes de artifacts recentes |
| Zensical muda rapidamente | Fixar versão compatível e atualizar deliberadamente |
| Release workflow duplica publicação | Semantic Release só versiona/tagueia; `wheels.yml` continua único publicador |
| Branch protection bloqueia commit de release | Testar permissão em dry-run; migrar para release PR se necessário |
| CHANGELOG existente é sobrescrito | Usar modo incremental e testar o diff em fixture antes de habilitar push |

## 14. Não objetivos desta rodada

- elevar cobertura arbitrariamente acima do baseline antes de fechar lacunas
  relevantes;
- adotar SonarCloud apenas para acumular badges;
- tornar o Event Bus distribuído ou alterar sua arquitetura;
- migrar a ferramenta de build Python/Rust;
- publicar documentação versionada por release com `mike`;
- publicar releases automaticamente a cada merge;
- editar workflows do `rokucontrol` além da remoção autorizada de artifacts
  antigos;
- garantir exatamente-once ou durabilidade contra energia sem `fsync=True`.

## 15. Fontes oficiais usadas no desenho

- [Zensical — criação e build](https://zensical.org/docs/create-your-site/)
- [Zensical — publicação no GitHub Pages](https://zensical.org/docs/publish-your-site/)
- [Zensical — configuração básica](https://zensical.org/docs/setup/basics/)
- [Codecov — autenticação e tokens](https://docs.codecov.com/docs/codecov-tokens)
- [Codecov — status badges](https://docs.codecov.com/docs/status-badges)
- [Python Semantic Release — configuração](https://python-semantic-release.readthedocs.io/en/latest/configuration/configuration.html)
- [GitHub — remoção de workflow artifacts](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/remove-workflow-artifacts)
- [GitHub — API de Actions artifacts](https://docs.github.com/en/rest/actions/artifacts)
- [Ruff — tutorial e integração](https://docs.astral.sh/ruff/tutorial/)
- [Pyrefly — configuração](https://pyrefly.org/en/docs/configuration/)
- [cargo-deny — checks](https://embarkstudios.github.io/cargo-deny/checks/index.html)
- [Gitleaks](https://github.com/gitleaks/gitleaks)

## 16. Gate para iniciar

Nenhum item acima será implementado até este plano ser revisado e aprovado.
Após aprovação, a execução começa pelo PR 1 e o checklist em `tasks/todo.md`
será atualizado ao final de cada incremento.
