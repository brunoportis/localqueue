## ADDED Requirements

### Requirement: Public API compatibility is preserved
The system SHALL preserve existing documented public APIs while internal queue and worker domain behavior is reorganized.

#### Scenario: Existing queue usage remains valid
- **WHEN** a caller constructs and uses `PersistentQueue("jobs")` with existing put, get, ack, release, dead-letter, stats, and subscriber operations
- **THEN** the observable behavior and supported call signatures remain compatible with the behavior before this change

#### Scenario: Existing worker usage remains valid
- **WHEN** a caller uses `persistent_worker`, `persistent_async_worker`, or `PersistentWorkerConfig` with existing arguments
- **THEN** the worker continues to process, retry, acknowledge, release, or dead-letter messages according to the existing public contract

### Requirement: Domain behavior has explicit internal ownership
The system SHALL move non-trivial queue and worker domain behavior out of anonymous procedural helper clusters and into named internal collaborators or strategy-style boundaries.

#### Scenario: Domain helpers are classified
- **WHEN** an internal helper coordinates idempotency, result caching, commit effects, worker outcomes, fanout, notification, dispatch, or policy compatibility
- **THEN** the behavior is owned by a named internal domain boundary rather than remaining as unrelated procedural glue

#### Scenario: Technical utilities remain simple
- **WHEN** an internal helper only performs technical work such as timing arithmetic, argument normalization, or small formatting
- **THEN** the helper can remain a utility without being promoted to a domain collaborator

### Requirement: Worker outcomes are coordinated consistently
The system SHALL centralize worker outcome decisions so successful, retryable, exhausted, and permanent failures follow one consistent domain path.

#### Scenario: Successful worker handling
- **WHEN** a worker handler completes successfully
- **THEN** idempotency completion, result persistence, commit effects, worker policy state, and queue acknowledgement are applied in the existing order and with the existing observable effects

#### Scenario: Failed worker handling
- **WHEN** a worker handler fails with retryable, exhausted, or permanent failure semantics
- **THEN** idempotency failure state, saga compensation when configured, release or dead-letter behavior, error payloads, and worker policy state match the existing observable behavior

### Requirement: Commit and result behavior remains policy-driven
The system SHALL execute local, outbox, two-phase, saga, and stored-result behavior according to the active delivery, result, and commit policies.

#### Scenario: Commit mode is executed by policy intent
- **WHEN** an effectively-once queue uses a configured commit policy
- **THEN** the runtime applies the corresponding local, outbox, two-phase, or saga effect without changing the public policy configuration model

#### Scenario: Cached results remain available
- **WHEN** a duplicate idempotent message has a succeeded record and a result policy that returns cached results
- **THEN** the worker returns the cached result and acknowledges the duplicate message as before

### Requirement: Queue facade delegates without changing semantics
The system SHALL keep `PersistentQueue` as the public facade while allowing internal operation flow to be delegated to focused collaborators.

#### Scenario: Queue operation semantics are preserved
- **WHEN** queue operations are internally delegated
- **THEN** leasing, acknowledgement timing, backpressure, fanout, deduplication, priority validation, notification, dispatch, and store interactions preserve existing observable behavior

#### Scenario: Store protocol remains the persistence boundary
- **WHEN** queue behavior needs persistence
- **THEN** it continues to use the existing queue, retry, idempotency, and result store protocols rather than bypassing them with special-case storage logic

### Requirement: Refactor is protected by behavioral tests
The system SHALL include tests that verify the public behavior affected by the internal architecture change.

#### Scenario: Public behavior is tested after extraction
- **WHEN** internal collaborators replace existing helper logic
- **THEN** tests cover queue facade behavior, worker success and failure outcomes, idempotency/result behavior, and commit-mode effects without requiring callers to know the private collaborator names
