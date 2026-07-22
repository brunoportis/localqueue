# Deterministic storage crash harness

Issue #18 adds deterministic, test-only evidence for SQLite transaction
boundaries. The Cargo feature is named `__crash_test`, has no default
activation, and is not used by the normal test, release, or wheels workflows.
It is not a runtime flag and is not supported in production.

## Protocol

The parent creates the database, closes its queue, binds a loopback TCP
listener, and starts one child. The child configures exactly one failpoint
through the test-only native method. Rust connects to the listener when the
point is reached, sends its stable name, and blocks reading one byte. Only
after the parent receives and validates that notification does it kill the
child with SIGKILL. A separate validator process then opens the database and
runs `PRAGMA integrity_check` plus deterministic status counts. A timeout is
only a synchronization failure; it is never used to coordinate the scenario.

The socket protocol is portable, but the complete process-kill contract is
currently Linux-only. Control scenarios run on every supported platform. The
dedicated Linux CI job runs all crash scenarios; non-Linux pytest runs report
an explicit platform skip for those scenarios rather than silently claiming
coverage.

## Scenarios and expected state after reopen

| Scenario | Initial state | Interrupted operation | Expected state |
| --- | --- | --- | --- |
| `enqueue-after-begin` | empty | BEGIN before INSERT | empty |
| `enqueue-before-commit` | empty | INSERT before COMMIT | empty |
| `claim-before-commit` | ready=1 | lease update before COMMIT | ready=1 |
| `ack-before-commit` | processing=1 | ACK update before COMMIT | processing=1 |
| `nack-before-commit` | processing=1 | NACK update before COMMIT | processing=1 |
| `fail-before-commit` | processing=1 | fail update before COMMIT | processing=1 |

The control scenarios run the corresponding committed transition and validate
that it survives close and reopen. An uncommitted ACK/NACK/fail therefore does
not create a terminal state or a ready state, and an uncommitted claim does
not leave a permanent lease. The report always requires exactly `ok` from
`integrity_check`.

## Evidence

Run one scenario outside pytest:

```bash
uv run maturin develop --locked --features __crash_test
python tests/crash_harness.py \
  --scenario enqueue-before-commit \
  --output result.json
```

Reports use schema version 1. A representative result is:

```json
{
  "schema_version": 1,
  "scenario": "enqueue-before-commit",
  "failpoint": "enqueue-before-commit",
  "operation": "enqueue",
  "platform": "linux",
  "child_exit_mode": "signal",
  "child_return_code": -9,
  "signal": "SIGKILL",
  "synchronization_reached": true,
  "integrity_check": "ok",
  "expected_counts": {"ready": 0, "processing": 0, "acked": 0, "failed": 0},
  "observed_counts": {"ready": 0, "processing": 0, "acked": 0, "failed": 0},
  "invariants": [{"name": "logical_counts_match", "passed": true}],
  "passed": true
}
```

SIGKILL is abrupt process termination, not physical power-loss evidence. It
does not cover disk firmware, host caches, a power cut, or filesystem/media
corruption. Those hardware and operational questions belong to issue #19/#31.
