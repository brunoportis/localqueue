## Context

`localqueue` already exposes strong domain names: queue policies, stores, adapters, retries, idempotency records, results, commit policies, worker configs, and specs. The current risk is that some of these concepts are still executed through private helper functions in queue and worker runtime modules instead of through explicit internal boundaries.

The main pressure points are:

- `PersistentQueue` owns public API behavior, policy resolution, store access, local synchronization, fanout, notification, and dispatch hooks.
- Worker runtime helpers coordinate idempotency, result caching, commit modes, saga/outbox/two-phase effects, failure classification, ack, release, and dead-letter transitions.
- Some compatibility modules reexport private helpers, making internal cleanup harder if those names become relied upon.

This change should improve internal architecture while preserving the user-facing simplicity that makes the project useful.

## Goals / Non-Goals

**Goals:**

- Preserve existing public API behavior and documented imports.
- Give domain behaviors explicit owners where helpers currently encode important concepts.
- Keep `PersistentQueue` as the simple public facade while reducing internal orchestration.
- Separate technical utilities from domain collaborators.
- Make worker outcome handling easier to test as domain behavior.
- Support future policy compatibility checks and adapter growth without adding procedural branches to central modules.

**Non-Goals:**

- No public API redesign.
- No new queue backend, broker protocol, remote coordination, or external dependency.
- No promise of generic exactly-once delivery beyond the existing effectively-once/idempotency model.
- No broad DDD rewrite or class extraction for purely mechanical helpers.
- No removal of compatibility aliases in this change.

## Decisions

### Keep `PersistentQueue` as the facade

`PersistentQueue` remains the public entry point for enqueue, lease, ack, release, dead-letter, stats, subscribers, and worker integration. Internally, queue construction and operations can delegate to smaller collaborators, but callers should not need to learn a new object model.

Alternative considered: split `PersistentQueue` into several public services. That would make the internal model more explicit, but it would hurt the simple `PersistentQueue("jobs")` experience and create unnecessary API churn.

### Extract behavior only when the helper represents a domain concept

Helpers such as deadline math and wait-time calculation can remain technical utilities. Helpers that coordinate idempotency, result caching, commit effects, worker outcome transitions, fanout semantics, or policy compatibility should move behind named internal collaborators.

Alternative considered: eliminate all private helpers. That would create classes without clear ownership and make the code more abstract without reducing real complexity.

### Introduce internal coordinators for worker domain behavior

Worker runtime should delegate idempotency and commit/result coordination to explicit internal collaborators. Likely candidates are:

- an idempotency handling coordinator for begin/complete/cached-result behavior
- a commit coordinator for local, outbox, two-phase, and saga modes
- an outcome coordinator for ack/release/dead-letter decisions after handler execution

These collaborators should be private implementation details unless a later change proves that users need extension points.

Alternative considered: move behavior directly onto existing policy dataclasses. That may be useful later, but policies currently act as compact value objects and public configuration. Adding side-effecting behavior to them now could blur their role and make serialization/documentation less clear.

### Keep policy objects descriptive until behavior needs stable extension points

Policy objects should continue to describe guarantees and configuration. Runtime collaborators can consume policies and stores to execute behavior. This preserves the current policy-as-configuration style while still reducing procedural branching in the worker.

Alternative considered: use a full strategy hierarchy for every policy immediately. That would increase implementation surface before the project has enough concrete adapter and compatibility cases to justify it.

### Protect architecture with behavior tests, not only shape tests

The refactor should be validated by existing queue, retry, CLI, and worker tests plus focused tests around idempotency/result/commit outcomes. Tests should assert observable behavior rather than private class names.

Alternative considered: test only new internal collaborators. That would make refactoring easier to write, but it could miss compatibility regressions in the public facade.

## Risks / Trade-offs

- Public compatibility can be accidentally narrowed -> keep existing documented imports and run the full test suite after each slice.
- Internal collaborators can become premature abstractions -> extract only helpers with domain vocabulary and repeated decision logic.
- Commit/idempotency behavior can drift during movement -> add focused tests before or alongside extraction.
- Private helper reexports may constrain cleanup -> treat reexport removal or deprecation as a separate change unless it is proven internal-only.
- More internal files can make navigation harder -> name collaborators after domain decisions rather than implementation mechanics.

## Migration Plan

1. Add focused behavior tests around current worker idempotency, result, commit, and failure outcome behavior.
2. Extract internal collaborators behind existing public functions and decorators.
3. Keep compatibility imports working during the refactor.
4. Run type checking, linting, and tests.
5. Update architecture/stability documentation only if the final boundaries affect documented behavior or maintenance guidance.

Rollback is straightforward because the change is internal: revert collaborator extraction while keeping any useful behavior tests.

## Open Questions

- Should private helper reexports from `localqueue.queue` be kept indefinitely, deprecated, or documented as unsupported internals in a later compatibility change?
- Should policy compatibility validation become part of this change, or remain a separate follow-up aligned with the queue architecture roadmap?
- Should commit behavior eventually live on policy objects, or should policies stay descriptive while coordinators execute behavior?
