# Plano: contrato e escala da ingestão com capacity

## Objetivo

Registrar um follow-up não bloqueante para validar a escala de
`enforce_capacity_policies` e decidir se o lookup de identidades deve migrar
de N point queries para uma operação set-based. A implementação atual é
entregável com as limitações públicas já documentadas; esta investigação
orienta a recomendação operacional e uma possível otimização posterior.

Este plano não bloqueia o merge do PR atual. Ele só deve reabrir a decisão se
um benchmark representativo demonstrar uma falha operacional concreta.

## Decisões já tomadas

- Backpressure sem timeout interno pode esperar indefinidamente.
- O timeout será composto externamente com `asyncio.timeout()` ou
  `asyncio.wait_for()`; não será criado `max_attempts`.
- Se o timeout cancelar uma transação nativa em voo, o erro só chega ao
  chamador depois que essa transação termina.
- Otimização Rust não será feita por intuição: primeiro será medido o banco
  pré-carregado e os casos de identidade nova, duplicada e conflitante.
- O benchmark atual de 10 mil linhas permanece como baseline preliminar, não
  como evidência de escala.
- A investigação de escala é follow-up pós-merge ou evidência de release, não
  critério de aceite deste PR.

## Dependências

```text
Contrato de espera/cancelamento
              │
              v
Benchmark reproduzível com banco pré-carregado
              │
              v
Comparação com critérios objetivos
        ┌─────┴─────┐
        │           │
   suficiente   insuficiente
        │           │
   documentar   protótipo set-based
                    │
                    v
             benchmark A/B + decisão
```

## Fase 1 — Fechar o contrato operacional

### Tarefa 1: Documentar espera indefinida e timeout externo

**Descrição:** Manter explícito que uma fila permanentemente cheia mantém
`ingest()` aguardando até haver capacity, cancelamento ou timeout externo.

**Critérios de aceite:**

- A documentação diz explicitamente que a espera pode ser indefinida.
- Há exemplos válidos com `asyncio.timeout()` e cancelamento da task.
- A documentação esclarece que um commit nativo em voo posterga a observação
  do timeout até a transação terminar.

**Verificação:**

- Teste da documentação verifica as frases contratuais essenciais.
- Build da documentação passa.

**Arquivos prováveis:** `docs/event-bus.md`,
`tests/test_operational_envelope_docs.py` ou teste documental equivalente.

**Escopo:** pequeno.

### Checkpoint 1

- Contrato revisado sem introduzir parâmetro novo na API.
- Testes e documentação passam.

## Fase 2 — Follow-up não bloqueante: medir o caminho predominante

Esta fase é executada depois do merge ou como parte da preparação de release.
Os resultados refinam a recomendação de escala; não impedem o merge da
implementação atual sem evidência de uma falha operacional.

### Tarefa 2: Tornar o benchmark configurável para escala e estado do banco

**Descrição:** Expandir o harness de ingestion para pré-carregar o banco e
separar o conjunto medido do setup.

**Dimensões a cobrir, sem produto cartesiano completo:**

- banco inicial: vazio, 100 mil, 1 milhão e 6 milhões de identidades;
- identidade do lote: nova, já persistida e conflito de fingerprint;
- capacity: desligada e ligada;
- fan-out: 1 e 5;
- batch: 100, 1.000 e 10.000;
- comparação: `dispatch()` repetido e ingestion nativa sem capacity.

**Critérios de aceite:**

- Setup e medição são fases distintas; preload não entra no throughput.
- Cada cardinalidade cria um banco-base uma vez; cenários derivados usam clone
  copy-on-write/reflink quando disponível, ou cópia convencional como
  fallback.
- A seleção de cenários evita recriar o preload para cada combinação e a
  configuração fica registrada no JSON.
- O benchmark aceita seleção de subset para smoke local e matriz release.

**Verificação:**

- Testes unitários validam a composição da matriz e os metadados.
- Smoke executa ao menos um cenário novo/duplicado em banco pré-carregado.

**Arquivos prováveis:** `benchmarks/ingestion_bench.py`,
`tests/test_ingestion_benchmark.py`, `benchmarks/README.md`.

**Escopo:** médio.

### Tarefa 3: Medir custo SQLite e duração do writer lock

**Descrição:** Registrar métricas que separem preparação Python do tempo
segurando `BEGIN IMMEDIATE`.

**Métricas obrigatórias:**

- itens e deliveries por segundo;
- duração total e p50/p95/max da transação;
- tempo aproximado sob writer lock;
- número de identidades distintas e consultas de existência;
- pico RSS, pico Python e tamanho DB/WAL/SHM;
- inserts, deduplicações e conflitos.

**Critérios de aceite:**

- O relatório permite comparar capacity on/off para a mesma configuração.
- O tempo de writer lock é medido no Rust, não inferido apenas da chamada
  Python.
- Ambiente, versão e commit aparecem no relatório.

**Verificação:**

- Testes Rust validam início/fim da instrumentação sem alterar atomicidade.
- O renderer rejeita relatórios incompletos.

**Arquivos prováveis:** `src/storage.rs`, binding de benchmark privado,
`benchmarks/ingestion_bench.py`, testes Rust/Python.

**Escopo:** médio.

### Checkpoint 2 — GO/NO-GO da otimização

Executar primeiro uma matriz curta em 100 mil e, se estável, cenários
representativos em 1 milhão e 6 milhões. A matriz release deve selecionar os
casos que mais distinguem point queries de lookup set-based; não deve executar
todas as combinações possíveis.

**Registrar a implementação atual como adequada para a recomendação medida se:**

- não houver crescimento aproximadamente linear do tempo por batch com o
  número de identidades distintas além do custo esperado de inserts;
- p95 do writer lock no batch recomendado de 1.000 permanecer dentro do
  orçamento definido para o projeto;
- capacity não causar degradação desproporcional em relação ao caminho sem
  capacity;
- memória e arquivos SQLite permanecerem dentro do envelope operacional.

Os valores numéricos do orçamento devem ser aprovados antes da matriz release;
o plano não inventa um SLA depois de ver os resultados.

## Fase 3 — Otimizar somente se o checkpoint recomendar

### Tarefa 4: Prototipar lookup set-based por queue

**Descrição:** Substituir os `SELECT EXISTS` individuais por lookup em chunks,
mantendo a semântica atual de `dedup_key`, `job_id` e repetições internas.

**Abordagem recomendada:**

1. Normalizar identidades distintas por queue no Rust.
2. Separar `dedup_key` e `job_id`.
3. Inserir as identidades do batch em uma tabela temporária reutilizável, ou
   usar CTE `VALUES` em chunks abaixo do limite de parâmetros.
4. Fazer joins set-based contra `messages`.
5. Calcular `new_rows` por queue antes do `COUNT` de READY/LEASED.

Tabela temporária é a primeira opção a prototipar para batches grandes; CTE em
chunks é a alternativa se lifecycle/limpeza da temp table complicar a conexão.

**Critérios de aceite:**

- Resultado idêntico para identidades novas, duplicadas no batch, já
  persistidas e conflitos de fingerprint.
- Atomicidade multi-queue e `Full`/`FullImpossible` permanecem inalterados.
- Nenhuma interpolação de valores em SQL; parâmetros continuam vinculados.

**Verificação:**

- Testes diferenciais executam implementação antiga e nova sobre casos
  gerados e comparam outcomes/erros.
- Testes cobrem batches acima do limite de parâmetros do SQLite.
- Crash failpoints antes do commit continuam atômicos.

**Arquivos prováveis:** `src/storage.rs` e testes Rust adjacentes.

**Escopo:** médio.

### Tarefa 5: Benchmark A/B e escolha final

**Descrição:** Rodar a mesma matriz contra point queries e implementação
set-based.

**Critérios de aceite:**

- A variante nova reduz materialmente p95 do writer lock no caso
  identity+capacity sem regressão relevante no caso pequeno.
- Throughput, memória, tamanho do banco e conflitos são comparados no mesmo
  hardware e commit.
- Se não houver ganho claro, o protótipo não entra no hot path.

**Verificação:** relatório JSON e resumo Markdown versionados como evidência.

**Arquivos prováveis:** benchmark, artifact de resultado e PR body.

**Escopo:** pequeno após a Tarefa 4.

## Fase 4 — Fechamento

### Tarefa 6: Atualizar contrato, recomendação e PR

**Critérios de aceite:**

- Documentação recomenda batch com base nos dados, sem prometer SLA universal.
- PR body liga diretamente para a evidência final e registra limitações.
- Suítes Python/Rust, lint, tipos, docs e crash-test passam.

## Riscos e mitigação

| Risco | Impacto | Mitigação |
| --- | --- | --- |
| Matriz de 6 milhões ser cara | Alto | Smoke 100 mil antes; subset configurável; execução release separada |
| Benchmark medir preload | Alto | Fases explícitas de setup e medição |
| Métrica Python esconder lock | Alto | Instrumentação no limite da transação Rust |
| Temp table vazar estado | Alto | Limpeza transacional e teste por conexão reutilizada |
| CTE exceder parâmetros | Médio | Chunk size derivado do limite SQLite |
| Otimização mudar dedup | Alto | Teste diferencial e casos de fingerprint |
| Timeout parecer imediato | Médio | Documentar espera por commit nativo em voo |

## Questões para aprovação

1. Qual orçamento de p95 do writer lock será usado no batch recomendado de
   1.000: 100 ms, 250 ms ou outro valor?
2. A matriz de 6 milhões será evidência de release ou acompanhamento
   pós-merge?
