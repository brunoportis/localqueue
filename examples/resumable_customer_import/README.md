# Resumable bulk customer import

An executable, self-contained example of a durable, resumable bulk customer
import built **only with public localqueue APIs** — no queue, checkpoint,
retry, deduplication, backpressure, or recovery infrastructure is written
here.

Pipeline:

```
customers.csv ──CsvSource──▶ EventBus.execute(source) ──▶ customer-creator
                                                                   │ returns
                                                                   ▼
                                          CustomerCreated ──▶ customer-audit
```

- `events.py` — `CustomerCreationRequested` with durable operation-scoped
  identity `(import_id, external_id)`, and `CustomerCreated` (audit record,
  intentionally without identity: no duplicate scenario requires it).
- `topology.py` — shared `BusTopology` imported by both sides; the producer
  never imports worker handlers.
- `run.py` — primary one-process operation: declares `CsvSource`, registers
  both handlers, configures concurrency/retry, awaits `bus.execute(source)`,
  prints the durable `ExecutionResult`, and calls `raise_for_failures()`.
- `producer.py` and `worker.py` — advanced separate producer/worker
  deployment using the same database and shared builders.
  The CSV schema is
  `external_id,name,email,phone` — there is no `import_id` column; the
  `--import-id` CLI value is the logical import identity and is injected
  into every `CustomerCreationRequested` by the transform.
- `worker.py` — typed `HandlerContext` with a `context_factory` injecting the
  API adapter per attempt; validation → `Reject`, temporary failure →
  `Retry`, rate limit → `Retry(after=...)`; returns `CustomerCreated` and
  lets the bus do the atomic local ACK + fan-out. The creator subscription
  runs with process-local concurrency 20
  (`bus.subscription(CUSTOMER_CREATOR, concurrency=20)`), so up to 20
  customer creations are in flight per worker process; the audit
  subscription keeps the default concurrency and is independent.
- `demo_api.py` — deterministic in-memory async `DemoCustomerApi` behind a
  small `CustomerApi` protocol. No server, no dependencies.

## 1. Environment setup (from the repo root)

```bash
uv sync --extra bus
```

## 2. Run the complete operation

```bash
uv run python -m examples.resumable_customer_import.run
```

`execute()` auto-starts the registered creator and audit handlers. The
`CustomerCreated` returned by the creator is a durable descendant, so the
operation does not complete until its audit delivery is terminal too. State
lives in `examples/resumable_customer_import/data/` by default.

The report includes:

```
execution ID:              ...
resumed:                   False
source items committed:    6
events dispatched:         6
events unrouted:           0
deliveries inserted:       5
deliveries deduplicated:   1
deliveries total:          9
acknowledged:              8
failed:                    1
completed timestamp:       ...+00:00
```

The duplicate row collapses onto the same durable identity
`(import_id, external_id)` and payload after normalization, so it is
deduplicated instead of delivered twice. The worker logs one
`audit import=demo-v1 external_id=... customer_id=...` line per created
customer; `invalid@example.com` is rejected (validation), so the demo command
reports the terminal result and `raise_for_failures()` exits nonzero.
`flaky@example.com`
retries twice then succeeds, `throttled@example.com` waits out one
rate-limit delay then succeeds.

## 3. Separate producer and worker processes

The original deployment remains available without introducing a network
service. In terminal 1:

```bash
uv run python -m examples.resumable_customer_import.worker
```

In terminal 2:

```bash
uv run python -m examples.resumable_customer_import.producer
```

Both processes share the same SQLite database, topology, source transform,
handler builders, and retry policy.

## 4. Rerun — resume the durable operation

```bash
uv run python -m examples.resumable_customer_import.run
```

The checkpoint stored the cursor of the last committed batch; the rerun
opens the CSV at that cursor (a single O(1) `seek`, earlier records are
never replayed), resumes the same execution, and reports cumulative durable
counters rather than only work performed by this Python invocation.

## 5. Inspect the checkpoint

```bash
uv run python - <<'PY'
from localqueue.bus import EventBus
from examples.resumable_customer_import.topology import BUS_NAME, TOPOLOGY

bus = EventBus("examples/resumable_customer_import/data", name=BUS_NAME, topology=TOPOLOGY)
try:
    state = bus.checkpoint("customer-import:demo-v1").inspect()
    print(state)
finally:
    bus.close()
PY
```

`CheckpointState` reports the cursor, source fingerprint, version, and
committed item/batch counts.

## 6. Inspect rejected/failed deliveries

The validation failure is rejected permanently and lands in the
subscription's failed deliveries:

```bash
uv run python - <<'PY'
from localqueue.bus import EventBus
from examples.resumable_customer_import.topology import BUS_NAME, TOPOLOGY, CUSTOMER_CREATOR

bus = EventBus("examples/resumable_customer_import/data", name=BUS_NAME, topology=TOPOLOGY)
try:
    for failed in bus.subscription(CUSTOMER_CREATOR).list_failed():
        print(failed.event_type, failed.attempts, failed.failure_category, failed.last_error)
finally:
    bus.close()
PY
```

## 7. Reset the checkpoint

```python
bus.checkpoint("customer-import:demo-v1").reset()
```

Resetting deletes **only the stored position**. It does not delete
deliveries already committed to the subscription queues, and it does not
release durable event identities: re-ingesting the same file after a reset
re-reads every row, but rows already delivered are deduplicated by identity
(`deliveries_deduplicated` grows, `deliveries_inserted` does not).

## 8. Replace the CSV after a partial run — `SourceChanged`

The checkpoint records the source fingerprint. With the default automatic
fingerprint, modifying or replacing the file and rerunning the producer with
the same checkpoint raises `SourceChanged` **before any row is consumed** —
nothing is committed. Reset the checkpoint to re-ingest the new file (rows
already delivered are still deduplicated by identity, per section 7).

## 9. `max_pending` backpressure

`max_pending` is an ephemeral per-subscription-queue pending bound for one
ingestion run. Try it:

```bash
# stop the worker, then:
uv run python -m examples.resumable_customer_import.producer --max-pending 2
```

The producer does not fail: when a batch would exceed the bound, ingestion
waits with bounded async backoff until the worker drains the queue, so a
fast producer cannot overrun a slow consumer. (With a batch that can never
fit, the run splits it into order-preserving halves.)

## 10. Why the external API still needs idempotency

Local checkpointing and durable event identity make the *bus side* safe:
rows are not re-read after commit, and re-delivered events deduplicate. But
the call from the worker to an external customer service crosses a process
boundary. If the worker creates a customer and crashes before the bus ACK
commits, the delivery is retried and the create call repeats. That is why
the handler sends `idempotency_key=ctx.event_id`: the event ID is stable
across attempts, so the service can return the first result instead of
creating a duplicate. `DemoCustomerApi` keeps its idempotency keys in
memory, which is fine for a demo — **a real external service must persist
idempotency keys durably**, at least as long as retries are possible.

## Larger-file recipe

```bash
# generate 50k generic rows
uv run python - <<'PY'
import csv
with open("/tmp/big_customers.csv", "w", newline="") as fh:
    writer = csv.writer(fh)
    writer.writerow(["external_id", "name", "email", "phone"])
    for n in range(50_000):
        writer.writerow(
            [f"EXT-{n:06d}", f"Customer {n}",
             f"customer{n}@example.com", f"+1 555 {n % 10_000:04d}"]
        )
PY

# terminal 1
uv run python -m examples.resumable_customer_import.worker

# terminal 2: start, then interrupt (Ctrl+C) mid-run
uv run python -m examples.resumable_customer_import.producer \
    --csv /tmp/big_customers.csv --import-id bulk-v1 --batch-size 500

# rerun: the producer resumes from the last committed batch's cursor
uv run python -m examples.resumable_customer_import.producer \
    --csv /tmp/big_customers.csv --import-id bulk-v1 --batch-size 500
```

The rerun prints `resumed: True`, a non-`None` start cursor, and only the
remaining rows in `items read` — no row already committed is re-read or
re-delivered.
