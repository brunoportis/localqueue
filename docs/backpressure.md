# Bounded backlog and producer backpressure

`SimpleQueue` can enforce an optional job-count limit for one logical queue:

```python
from localqueue import Full, SimpleQueue


queue = SimpleQueue("./data", max_pending_jobs=10_000)
try:
    queue.put({"task": "send-email"}, block=False)
except Full:
    # Slow or reject this producer according to the application policy.
    ...
```

The default is `max_pending_jobs=None`, which is unlimited and preserves the
behavior of existing applications. A configured limit must be a positive
integer; zero, negative values, booleans, and floats are rejected. The limit is
scoped by `queue.name`, so two logical queues in the same database have
independent usage.

## What counts as pending

Capacity is the number of non-terminal jobs:

```text
pending_jobs = ready + processing
```

All ready jobs count, including delayed jobs whose `available_at` is in the
future. All processing jobs count, including active leases and expired leases
that have not yet been reclaimed. ACKed and failed jobs do not count.

| Transition | Capacity effect |
| --- | --- |
| enqueue a new row | consumes one slot |
| deduplicate by an existing `(queue, job_id)` | consumes no slot |
| ready → processing | unchanged |
| processing → ready by NACK or reclaim | unchanged |
| processing → failed after attempts are exhausted | releases one slot after commit |
| processing → ACKed or explicit `fail()` | releases one slot after commit |
| failed → ready by `retry_failed()` | consumes one slot |
| purge ACKed/failed rows | unchanged |

Opening an existing queue above a newly configured limit is allowed. No job is
deleted or failed automatically. Diagnostics reports zero available slots,
new rows remain blocked, and deduplicated puts remain allowed. New batches can
enter only after enough jobs reach terminal states for the complete batch.

This is a logical backlog limit. It does not limit the SQLite database file,
WAL, disk usage, payload bytes, or retained ACKed/failed rows. The physical
database can continue to grow while terminal records are retained.

## Blocking and timeouts

`block=False` makes one capacity attempt and raises the stable
`Full("queue is full")` exception if the batch cannot fit:

```python
queue.put(payload, block=False)
```

The default `block=True, timeout=None` waits indefinitely. A finite timeout
limits the total wait for capacity and uses a monotonic deadline:

```python
queue.put(payload, timeout=5.0)
```

A zero timeout makes one immediate attempt. Negative timeouts raise
`ValueError`. Payloads are serialized once before polling, including every
item passed to `put_many()`.

Waiting uses bounded exponential polling because capacity may be released by
another process. The first interval is 10 ms and the maximum is 250 ms. This
avoids a hot loop while bounding the normal delay after another process frees
a slot. A process-local close event interrupts waits in the same object; it is
not presented as a cross-process notification mechanism.

For a finite deadline, each native attempt temporarily caps that connection's
SQLite busy timeout to the smaller of 250 ms and the remaining deadline, then
restores the configured 5-second timeout. SQLite `BUSY`, `LOCKED`, I/O, and
disk-full errors remain `LocalQueueError` failures and are never converted to
`Full`. `Full` means only that the configured logical capacity is exhausted.

If another thread calls `queue.close()` during a capacity wait, the wait ends
promptly with `LocalQueueError("queue is closed")` and does not enqueue later.

## Atomicity, batches, and deduplication

Capacity enforcement and insertion share one SQLite writer transaction:

1. `BEGIN IMMEDIATE` acquires the database writer lock.
2. The native engine counts ready and processing rows for this logical queue,
   using the existing queue/status index.
3. It calculates the rows the input would actually create.
4. It rejects an over-capacity input or inserts the complete input.
5. It commits once.

There is no count in Python, no reservation table, and no gap between the
capacity check and insert. Participating producer processes therefore cannot
oversubscribe the configured limit.

For capacity calculation, every item without `job_id` is new. A `job_id` that
already exists in the same logical queue needs no slot. Repeated new `job_id`
values in one batch need one slot per distinct value. The check runs under the
same writer lock used by insertion.

`put_many()` is all or nothing: an over-capacity batch raises `Full` without
writing a prefix or advancing the SQLite ID sequence. It is not split to fit.
An empty batch still returns `[]` when the queue is full.

## Retrying failed jobs

`retry_failed(message_id)` remains non-blocking. It first verifies under
`BEGIN IMMEDIATE` that the selected row exists in `failed`, then checks
capacity and moves it to ready in that transaction. A missing or non-failed ID
continues to raise `LocalQueueError("job not found")`; a valid failed row raises
`Full` when no slot is available.

## Configuration across processes

`max_pending_jobs` is configuration of each `SimpleQueue` object, like lease,
retry, and fsync settings. It is not stored in SQLite and adds no schema
migration. All participating producers for the same logical queue must use the
same limit.

A producer opened with `max_pending_jobs=None` is unlimited. The effective
limit cannot protect against a misconfigured client or direct SQL writes. The
multiprocess guarantee applies to producers participating in the same
configured contract on one machine and local store.

EventBus fanout remains unlimited in this release. `EventBus` has no capacity
option, and the dispatch queue's `NativeQueue` policy is not applied to target
subscription queues.
