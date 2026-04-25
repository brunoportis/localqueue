# Guarantee Matrix

This matrix describes the guarantees provided by various combinations of queue
policies.

| Combination | Guarantee | Failures Handled | Failure Window |
| :--- | :--- | :--- | :--- |
| **Simple Pull** (Default) | At-least-once | Consumer crash, network | Between lease and ack |
| **At-most-once** | Best-effort | None | Before delivery |
| **Effectively-once** (Local) | Effectively-once | Duplicates, crashes | After success, before commit |
| **Push** (In-process) | Best-effort | None | Producer crash |
| **Transactional Outbox** | At-least-once | Producer crash | Between DB commit and enqueue |

## Policy Compatibility

| Policy A | Policy B | Compatibility | Note |
| :--- | :--- | :--- | :--- |
| `PushConsumption` | `NoDispatcher` AND `NoNotification` | Risky | Messages enqueued but never handled or signaled |
| `PublishSubscribe` | `NoSubscriptions` | Risky | Messages fanned out to zero subscribers |
| `PriorityOrdering` | Priority = 0 | Documentary | Priority 0 behaves like FIFO |

## Future Validation

Risky combinations should eventually trigger a warning or a `ValueError` in the
`PersistentQueue` constructor.
