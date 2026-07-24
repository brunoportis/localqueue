# Python quality contracts

This document records the baseline used to introduce structural and typing guardrails for the public Python package in `v1.3.0`.

The baseline was measured on the `v1.2.0` release commit `8d1e5abd034ba0f0d89a977f7b2eca9d4dc0a3ef` on 2026-07-24. These numbers are ratchets: they establish the maximum accepted debt while follow-up work reduces it. Raising a threshold requires an explicit design rationale.

## Scope

The blocking structural contract applies to production package code under `python/localqueue`.

Repository-wide Ruff correctness, import, and formatting checks continue to use the normal `pyproject.toml` configuration. Complexity limits use the separate `ruff-runtime.toml` configuration so test code, release tooling, compatibility harnesses, and chaos campaigns are not judged as runtime APIs.

The benchmark package remains visible in Radon reports. Existing benchmark hotspots have exact, rule-scoped Ruff exceptions rather than a package-wide lint exclusion.

## Ruff baseline

The initial audit used:

```bash
uv run ruff check python/localqueue \
  --preview \
  --select C901,PLR0911,PLR0912,PLR0913,PLR0915,PLR1702
```

It reported 30 findings:

| Rule | Findings |
| --- | ---: |
| `C901` cyclomatic complexity | 11 |
| `PLR0911` returns | 1 |
| `PLR0912` branches | 4 |
| `PLR0913` arguments | 9 |
| `PLR0915` statements | 5 |
| `PLR1702` nested blocks | 0 |

Twenty findings were in benchmark orchestration and rendering code. Ten were in the queue and EventBus runtime.

The measured runtime maxima became the initial blocking thresholds:

| Metric | Threshold |
| --- | ---: |
| Cyclomatic complexity | 17 |
| Arguments | 9 |
| Branches | 18 |
| Returns | 7 |
| Statements | 56 |
| Nested blocks | 5 |

The main runtime hotspots were:

- `SimpleQueue._wait_for_capacity`;
- `EventBus._register_handler`;
- `run_consumer`;
- `_process_delivery`.

Passing the gate does not declare those functions ideal. It prevents new code from making the current ceiling worse while later issues refactor the APIs and handlers.

Run the contracts locally with:

```bash
uv run ruff check .
uv run ruff format --check .
uv run ruff check python/localqueue --config ruff-runtime.toml
```

## Pyrefly baseline

Strict public type coverage was:

```text
89.83% displayed (362 of 403 typable public symbols; 89.8263% exact)
```

The initial blocking command used `89.82` so the rounded display value did not
exceed the exact baseline:

```bash
uv run pyrefly coverage check python/localqueue \
  --strict \
  --public-only \
  --fail-under 89.82
```

Each public symbol is about 0.25 percentage point at the current denominator, so losing even one typed symbol still fails this threshold.

The strict calculation does not count public symbols contaminated by `Any` as fully typed. This makes the coverage percentage useful as a non-regression ratchet while the generic API work removes known holes.

Issue #64 raised the measured strict public coverage to:

```text
91.50% (366 of 400 typable public symbols)
```

Its blocking threshold was `91.49`, immediately below the exact measured
value. Losing one currently typed symbol produced at most
`365/400 = 91.25%` and therefore failed that intermediate ratchet without
depending on the displayed rounding.

`explicit-any` is configured as a warning and surfaced in CI:

```bash
uv run pyrefly check --progress-bar no --min-severity warn
```

The baseline produced 110 warning-level diagnostics, predominantly explicit `Any` at dynamic and currently under-typed boundaries. Issue #64 owns the generic queue and handler redesign. This issue makes the debt visible and prevents public type coverage from declining; it does not hide the findings behind suppressions.

The installed Pyrefly version recognizes `explicit-any` but not the newer `no-any-return` diagnostic name. The configuration follows the repository's pinned tool range rather than documentation for an unreleased or newer interface.

## Radon baseline

Radon reported:

```text
204 analyzed blocks
Average complexity: A (4.12)
```

Notable runtime scores included:

- `SimpleQueue._wait_for_capacity`: C (15);
- `EventBus._register_handler`: C (14);
- `_process_delivery`: C (19);
- `run_consumer`: C (11).

The benchmark multiprocess harness is intentionally reported separately because its orchestration functions are much larger; `_execute_multiprocess_scenario` measured F (68). That evidence supports future benchmark refactoring but should not force a permissive runtime threshold.

Radon is reporting-only. Maintainability Index and aggregate complexity are useful trends, not automatic design verdicts.

Run reports locally without adding Radon to the locked project environment:

```bash
uvx --from 'radon>=6,<7' radon cc python/localqueue -s -a
uvx --from 'radon>=6,<7' radon mi python/localqueue -s
uvx --from 'radon>=6,<7' radon raw python/localqueue -s
```

CI uploads the three reports as a retained workflow artifact.

## Final v1.3.0 ratchet after #63–#65

The final ratchet was measured on post-#70 `main` commit
`d60c9688b3e2f81959b49c967905a573ad71b737` on 2026-07-24. The effective
tools were:

| Tool | Version |
| --- | --- |
| Python | 3.14.6 |
| Ruff | 0.15.22 |
| Pyrefly | 1.1.1 |
| Radon | 6.0.1 |

The clean measurement commands were:

```bash
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run ruff check python/localqueue --config ruff-runtime.toml
PYTHONPATH=python uv run pyrefly check --progress-bar no
PYTHONPATH=python uv run pyrefly check \
  --progress-bar no \
  --min-severity warn
PYTHONPATH=python uv run pyrefly coverage check \
  python/localqueue \
  --strict \
  --public-only \
  --fail-under 0
uvx --from 'radon>=6,<7' radon cc python/localqueue -s -a
uvx --from 'radon>=6,<7' radon mi python/localqueue -s
uvx --from 'radon>=6,<7' radon raw python/localqueue -s
```

### Ruff final ratchet

The maxima were extracted in one reproducible pass rather than by repeatedly
decrementing the production configuration. Two temporary Ruff configurations
set all six limits to zero and requested JSON output. The raw configuration
had no per-file ignores; the blocking configuration copied the effective
per-file ignores. Diagnostic locations were mapped to their smallest enclosing
function with the Python AST. The temporary files were discarded after the
measurement.

The raw view across all of `python/localqueue` was:

| Rule | Raw maximum | Limiting function |
| --- | ---: | --- |
| `C901` complexity | 24 | `benchmark.multiprocess._execute_multiprocess_scenario` |
| `PLR0911` returns | 6 | `benchmark.cli.main`; `bus.consumer._process_delivery` |
| `PLR0912` branches | 19 | `benchmark.multiprocess._execute_multiprocess_scenario` |
| `PLR0913` arguments | 11 | `benchmark.multiprocess.producer_target`; `consumer_target` |
| `PLR0915` statements | 111 | `benchmark.multiprocess._execute_multiprocess_scenario` |
| `PLR1702` nested blocks | 5 | `benchmark.multiprocess.producer_target`; `run_multiprocess_profile`; `bus.consumer.run_consumer` |

The blocking view, after only the still-justified exceptions, determines the
committed thresholds:

| Metric | Initial baseline | Current maximum | New threshold | Limiting function |
| --- | ---: | ---: | ---: | --- |
| Cyclomatic complexity (`C901`) | 17 | 17 | 17 | `bus.consumer.run_consumer` |
| Arguments (`PLR0913`) | 9 | 8 | 8 | `bus.bus.EventBus.__init__`; `benchmark.runner._scenario` |
| Branches (`PLR0912`) | 18 | 15 | 15 | `bus.consumer._process_delivery` |
| Returns (`PLR0911`) | 7 | 6 | 6 | `benchmark.cli.main`; `bus.consumer._process_delivery` |
| Statements (`PLR0915`) | 56 | 56 | 56 | `bus.consumer.run_consumer` |
| Nested blocks (`PLR1702`) | 5 | 5 | 5 | `benchmark.multiprocess.producer_target`; `run_multiprocess_profile`; `bus.consumer.run_consumer` |

Arguments, branches, and returns fell below their original ceilings after the
v1.3 API work, so their thresholds decrease without spare capacity.
Complexity, statements, and nesting remain unchanged because current code
still reaches those exact ceilings. A threshold is a maximum accepted local
debt, not a target or a claim that every function at the ceiling is ideal.

### Ruff exceptions

Every retained exception still violates the final blocking threshold when
measured without that exception:

| File | Rule | Function | Current value | Justification |
| --- | --- | --- | ---: | --- |
| `python/localqueue/benchmark/multiprocess.py` | `C901` | `_execute_multiprocess_scenario`; `run_multiprocess_profile` | 24; 18 | Existing spawn/process orchestration remains visible in Radon; a broad benchmark refactor is outside this ratchet. |
| `python/localqueue/benchmark/multiprocess.py` | `PLR0912` | `_execute_multiprocess_scenario`; `run_multiprocess_profile` | 19; 17 | Existing scenario orchestration exceeds the branch ceiling without justifying a higher runtime-wide limit. |
| `python/localqueue/benchmark/multiprocess.py` | `PLR0913` | `producer_target`; `consumer_target`; `_execute_multiprocess_scenario`; `run_multiprocess_scenario` | 11; 11; 10; 10 | Spawn targets and the scenario harness retain cohesive process inputs; parameter-object churn is outside this PR. |
| `python/localqueue/benchmark/multiprocess.py` | `PLR0915` | `_execute_multiprocess_scenario`; `run_multiprocess_profile` | 111; 59 | The existing reporting/orchestration hotspot remains advisory in Radon and does not widen the production contract. |
| `python/localqueue/benchmark/runner.py` | `PLR0912` | `run_profile` | 16 | The single-process scenario dispatcher is one branch above the runtime ceiling; refactoring the benchmark is not part of the ratchet. |

The following obsolete entries were removed:

| File | Removed rule | Current limiting function and value |
| --- | --- | --- |
| `python/localqueue/benchmark/cli.py` | `C901` | `main`: 11, below 17 |
| `python/localqueue/benchmark/metrics.py` | `PLR0913` | `MetricSummary.from_samples`: 6, below 8 |
| `python/localqueue/benchmark/render.py` | `C901` | `render_markdown`: 13, below 17 |
| `python/localqueue/benchmark/runner.py` | `C901` | `run_profile`: 16, below 17 |
| `python/localqueue/benchmark/runner.py` | `PLR0913` | `_scenario`: 8, equal to 8 |

These exceptions must not be reintroduced for convenience. A future exception
needs the same file-, rule-, function-, and value-level evidence.

### Pyrefly final ratchet

| Metric | Initial baseline | After #64 | After #65 | Final gate |
| --- | ---: | ---: | ---: | ---: |
| Strict public coverage | 362/403 (89.8263%) | 366/400 (91.50%) | 384/418 (91.8660287081…%) | 91.86 |
| Warning diagnostics | 110 | 76 | 76 | Visible, reporting-only |
| `explicit-any` | 107 | 73 | 73 | Visible, reporting-only |

The final threshold is the greatest two-decimal value that does not exceed the
mathematical baseline:

```text
384 / 418 × 100 = 91.86602870813397…%
floor to two decimal places = 91.86
```

The canonical command is:

```bash
PYTHONPATH=python uv run pyrefly coverage check \
  python/localqueue \
  --strict \
  --public-only \
  --fail-under 91.86
```

One lost fully typed symbol fails with margin:

```text
383 / 418 × 100 = 91.62679425837320…% < 91.86%
```

The denominator remains the real public API under `python/localqueue`; no
module, export, typing fixture, or public symbol was excluded to improve the
ratio.

### Warning and explicit-Any audit

| Diagnostic | Initial baseline | Current | Main files |
| --- | ---: | ---: | --- |
| `explicit-any` | 107 | 73 | `benchmark/multiprocess.py` 28; `runner.py` 14; `models.py` 12; `multiprocess_models.py` 7; `render.py` 5; `bus/event.py` 3; other benchmark files 4 |
| `unnecessary-type-conversion` | Not separately classified (3 non-`explicit-any` warnings total) | 3 | `benchmark/errors.py`; `benchmark/multiprocess.py`; `bus/bus.py` (one each) |
| Total warnings | 110 | 76 | 73 explicit `Any`; 3 unnecessary conversions |

Seventy remaining explicit `Any` diagnostics belong to the pre-existing
benchmark/report/process boundary. The other three are deliberate Pydantic
boundaries in `bus/event.py`: validated-data, class-creation keyword
arguments, and open-ended model construction. This ratchet does not rename,
alias, suppress, or replace those types merely to improve a count.

Pyrefly 1.1.1 can emit JSON, but the GitHub quality workflow deliberately
supports the project range `pyrefly>=1.1,<2` rather than a second exact pin or
a project-owned diagnostic-budget schema. Turning the 73 findings into a
blocking parser would therefore add a parallel pin/parser contract and
workflow redesign. The existing warning audit remains visible and
non-blocking; strict public coverage is the blocking public-API type contract.

### Radon after #70

| Metric | v1.2.0 baseline | Post-#70 main |
| --- | ---: | ---: |
| Analyzed blocks | 204 | 244 |
| Average complexity | A (4.12) | A (3.7663934426) |

Current runtime hotspots are:

- `bus.consumer._process_delivery`: C (16);
- `SimpleQueue._wait_for_capacity`: C (15);
- `EventBus._register_handler`: C (14);
- `bus.consumer.run_consumer`: C (11).

Current benchmark hotspots are:

- `_execute_multiprocess_scenario`: F (68);
- `run_multiprocess_profile`: D (28);
- `render_markdown`: D (26);
- `runner.run_profile`: D (23);
- `_fanout`: C (19).

Compared with v1.2.0, the package has 40 more analyzed blocks while average
complexity fell from 4.12 to about 3.77. The runtime `_process_delivery`
hotspot fell from 19 to 16; `_wait_for_capacity`, `_register_handler`, and
`run_consumer` remain at 15, 14, and 11. The benchmark maximum remains 68 and
continues to be reported even where an exact Ruff exception exists.

Radon remains advisory. Maintainability Index, raw LOC, Halstead metrics, and
aggregate complexity are not blocking gates and do not replace Ruff's
per-function structural contracts.

## Ratchet policy

- Lower thresholds when a refactor makes the previous ceiling obsolete.
- Do not increase thresholds merely to merge a new function.
- Treat `PLR0913` as a cohesion review, not a command to manufacture parameter objects.
- Keep exceptions exact by file and rule, with a written rationale.
- Do not use broad `noqa`, type ignores, or package-wide exclusions to preserve a green build.
- Radon reports remain advisory unless a future issue defines a specific, evidence-backed gate.
- Treat every threshold as a ceiling rather than a design goal.
- Require architectural justification before any future threshold increase.
