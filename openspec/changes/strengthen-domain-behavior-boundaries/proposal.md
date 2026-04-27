## Why

The project is stable and moving in a good direction, but several domain behaviors are still coordinated through private helpers inside queue and worker runtime modules. This makes future growth riskier because policies, commit handling, idempotency, and queue orchestration can become harder to evolve without expanding procedural glue.

## What Changes

- Introduce explicit internal boundaries for domain behavior currently concentrated in helper functions.
- Keep `PersistentQueue`, `QueueSpec`, worker decorators, policy objects, store protocols, adapters, and documented imports source-compatible.
- Move commit, idempotency, result, and worker outcome coordination behind named internal collaborators or strategy-style objects.
- Preserve the simple public queue API while reducing the amount of orchestration directly owned by `PersistentQueue` and worker runtime functions.
- Clarify which private helpers are purely technical utilities and which concepts deserve domain-level ownership.
- Add tests that protect public behavior during the refactor.

## Capabilities

### New Capabilities
- `domain-behavior-boundaries`: Defines the architectural behavior expected from internal queue and worker domain boundaries while preserving public API compatibility.

### Modified Capabilities
- None.

## Impact

- Affected code: `localqueue/queues/core.py`, `localqueue/workers/runtime.py`, `localqueue/workers/queue.py`, `localqueue/policies/*`, and related tests.
- Public API: no intended breaking changes.
- Dependencies: no new runtime dependencies expected.
- Documentation: may update architecture or stability docs to describe the new internal boundaries if implementation makes them user-relevant.
