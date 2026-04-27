## 1. Baseline Protection

- [x] 1.1 Identify current public API imports and documented compatibility names that must remain source-compatible.
- [x] 1.2 Add or tighten focused tests for `PersistentQueue` put/get/ack/release/dead-letter/stats/subscriber behavior before internal movement.
- [x] 1.3 Add or tighten focused tests for worker success, retryable failure, exhausted failure, permanent failure, and worker policy state behavior.
- [x] 1.4 Add or tighten focused tests for effectively-once idempotency, stored result return, outbox, two-phase, and saga commit effects.

## 2. Worker Domain Boundaries

- [x] 2.1 Extract idempotency begin, cached-result, and completion behavior into a named internal collaborator.
- [x] 2.2 Extract result and commit effect handling into a named internal collaborator that consumes existing result and commit policies.
- [x] 2.3 Extract worker success/failure outcome decisions into a named internal collaborator while preserving ack, release, dead-letter, and error payload behavior.
- [x] 2.4 Update synchronous and asynchronous worker paths to use the same internal domain collaborators where practical.

## 3. Queue Facade Boundaries

- [x] 3.1 Classify existing queue helpers as technical utilities or domain behavior.
- [x] 3.2 Move fanout or post-put domain behavior behind focused internal operations if it reduces orchestration inside `PersistentQueue`.
- [x] 3.3 Keep timing, validation, and argument-normalization utilities simple unless they carry domain decisions.
- [x] 3.4 Preserve `PersistentQueue` as the public facade and keep existing supported call signatures unchanged.

## 4. Compatibility And Documentation

- [x] 4.1 Keep documented public imports and compatibility aliases working after the internal refactor.
- [x] 4.2 Avoid removing private helper reexports in this change unless tests prove they are not externally exposed.
- [x] 4.3 Update architecture or stability documentation if the final internal boundaries change documented maintenance guidance.
- [x] 4.4 Capture any remaining private-helper reexport cleanup as a separate follow-up if needed.

## 5. Verification

- [x] 5.1 Run `uv run ruff check localqueue tests`.
- [x] 5.2 Run `uv run basedpyright`.
- [x] 5.3 Run `uv run pytest`.
- [x] 5.4 Review the final diff to confirm the change is internal, behavior-preserving, and scoped to the proposal.
