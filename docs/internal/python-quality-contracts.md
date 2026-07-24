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

The blocking threshold is now `91.49`, immediately below the exact measured
value. Losing one currently typed symbol produces at most `365/400 = 91.25%`
and therefore fails the ratchet without depending on the displayed rounding.

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

## Ratchet policy

- Lower thresholds when a refactor makes the previous ceiling obsolete.
- Do not increase thresholds merely to merge a new function.
- Treat `PLR0913` as a cohesion review, not a command to manufacture parameter objects.
- Keep exceptions exact by file and rule, with a written rationale.
- Do not use broad `noqa`, type ignores, or package-wide exclusions to preserve a green build.
- Radon reports remain advisory unless a future issue defines a specific, evidence-backed gate.
