# Architecture import contracts

Import Linter 2.x makes the existing production-package boundaries executable.
It does not introduce a new directory architecture or change runtime behavior.

## Direction

```text
benchmark -> bus -> core/runtime
```

The arrows are allowed import directions. `benchmark` may consume the EventBus
and core runtime; `bus` may consume core runtime; neither core/runtime nor the
EventBus may consume a layer above it.

The exhaustive `production-dependency-direction` contract classifies every
Import-Linter-visible, first-level child of `localqueue`:

- `benchmark`;
- `bus`;
- `core`, `deadletter`, `diagnostics`, `exceptions`, `job`, `maintenance`,
  `policies`, and `worker` as non-independent core/runtime siblings.

Siblings are separated by `:` so they can retain legitimate dependencies such
as `worker -> core`, `job -> core`, and `core -> diagnostics`. A new Python
module directly under `localqueue` fails the exhaustive contract until it is
deliberately classified.

`localqueue.localqueue` is a compiled native extension with a `.pyi` stub.
Import Linter 2.13 does not expose it as a module in this graph, so it is not a
layer and it needs no exhaustiveness exception. Its direct consumers (core,
diagnostics, maintenance, EventBus, and benchmark/environment tooling) are
intentional. Native-extension access contract deferred: current direct consumers
are intentional and no adapter boundary exists yet.

## Facade and optional dependencies

The `facade-does-not-load-optional-packages` contract treats the exact
`localqueue` module as a module, not as all of its descendants. It rejects
direct and indirect imports from the facade to `localqueue.bus`,
`localqueue.benchmark`, and `pydantic`. This keeps `import localqueue` usable
without the `bus` extra and without benchmark tooling.

The `pydantic-is-local-to-eventbus` contract covers all of `localqueue` and
forbids Pydantic except for these direct, intentional EventBus edges:

- `localqueue.bus -> pydantic`: preserves the existing missing-extra error;
- `localqueue.bus.event -> pydantic`: defines validated event models;
- `localqueue.bus.envelope -> pydantic`: validates persisted event envelopes.

Each allowance is an exact importer-to-imported edge. `unmatched_ignore_imports_alerting = "error"` makes a stale allowance fail, and no allowance
uses a wildcard. Indirect imports remain checked; the contracts do not enable
`allow_indirect_imports`.

Imports guarded by `TYPE_CHECKING` remain in the graph through
`exclude_type_checking_imports = false`. A type-only import still describes an
architectural dependency and cannot bypass a boundary.

## Run locally

After preparing the development extra, run:

```bash
PYTHONPATH=python uv run lint-imports
PYTHONPATH=python uv run lint-imports --verbose
```

Import Linter caches its graph in `.import_linter_cache/`, which is ignored by
Git. The CI gate uses the same compatible `import-linter>=2,<3` range as the
development extra.

## Evolution policy

Classify every new top-level Python module in the exhaustive layers contract.
A new forbidden dependency requires a deliberate architectural change and a
PR rationale; it must never be handled by a wildcard ignore. Exact Pydantic
allowances may change only when the corresponding direct import exists and its
optional-integration rationale is documented here.
