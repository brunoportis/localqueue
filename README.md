# localqueue

[![PyPI](https://img.shields.io/pypi/v/localqueue.svg)](https://pypi.org/project/localqueue/)
[![Python](https://img.shields.io/pypi/pyversions/localqueue.svg?cacheSeconds=300)](https://pypi.org/project/localqueue/)
[![CI](https://github.com/brunoportis/localqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/brunoportis/localqueue/graph/badge.svg)](https://app.codecov.io/gh/brunoportis/localqueue)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/brunoportis/localqueue/blob/main/LICENSE)

A persistent, multiprocess-safe local queue for Python, backed by SQLite and Rust.

`localqueue` gives scripts and workers durable jobs with ACK/NACK, leases,
bounded retries, and dead-letter handling—without a server, daemon, or external
service.

- **Durable:** jobs survive process restarts in a local SQLite database.
- **Safe under concurrency:** multiple Python threads and processes can share a
  queue on the same machine.
- **Failure-aware:** expired leases and transient failures are retried; exhausted
  jobs move to a dead-letter state.
- **Fenced:** every delivery has a unique receipt, so stale workers cannot
  acknowledge a newer delivery.
- **Flexible:** use multiple named queues, optional job deduplication, automatic
  worker heartbeats, and custom serializers.
- **Event-driven:** optional durable pub/sub with explicit static topology,
  atomic fan-out, consumer groups, retries, and dead-letter handling.

[Installation](#installation) · [Quick start](#quick-start) ·
[Worker](#worker) · [Event bus](#event-bus) ·
[Benchmarks](#benchmarks) ·
[Guarantees](#delivery-guarantees) ·
[Backpressure](#bounded-backlog-and-backpressure) ·
[Diagnostics](#runtime-diagnostics) · [API](#api-overview) ·
[Storage compatibility](docs/storage-compatibility.md) ·
[Operational envelope](docs/operational-envelope.md) ·
[Changelog](CHANGELOG.md) ·
[Development](#development)

## Installation

Using [uv](https://docs.astral.sh/uv/):

```bash
uv add localqueue
```

Using pip:

```bash
python -m pip install localqueue
```

`localqueue` requires Python 3.10 or newer.

Upgrading from 0.5.0 requires code and storage changes because 1.x is a
backward-incompatible reimplementation. See
[Migrating from 0.5.0](CHANGELOG.md#migrating-from-050).

## Quick start

Save the two labeled blocks as `producer.py` and `worker.py`, then run the
producer before starting the worker.

```python
# producer.py
from localqueue import SimpleQueue


with SimpleQueue("./data", lease_seconds=30, max_retries=3) as queue:
    queue.put(
        {"task": "send-email", "to": "hello@example.com"},
        job_id="welcome-email-42",
    )
```

The producer can stop here. `put()` commits the job to
`./data/localqueue.db`; it is not an in-memory dictionary.

```python
# worker.py — run this later, in another process
from localqueue import SimpleQueue

with SimpleQueue("./data", lease_seconds=30, max_retries=3) as queue:
    job = queue.get()

    try:
        print(f"Sending email to {job.data['to']}")
    except Exception as error:
        queue.nack(job, last_error=str(error))
    else:
        queue.ack(job)
```

The path passed to `SimpleQueue` is a directory. The queue creates and manages
`localqueue.db` inside it. Payloads are JSON-serialized by default. Jobs
survive normal process restarts; use `fsync=True` when the stronger SQLite
durability setting documented in [Delivery guarantees](#delivery-guarantees) is
required.

## Worker

`Worker` handles the ACK/NACK lifecycle for you. A successful handler is
acknowledged, an unexpected exception is retried, and an exception listed in
`permanent_errors` is sent directly to the dead-letter state.

```python
from localqueue import SimpleQueue, Worker


class InvalidDeployment(Exception):
    pass


def deploy(job):
    print(f"Deploying {job.data['app']}@{job.data['revision']}")


with SimpleQueue("./data", lease_seconds=30, max_retries=3) as queue:
    worker = Worker(
        queue,
        deploy,
        permanent_errors=(InvalidDeployment,),
        heartbeat_interval=10,
    )
    worker.run()
```

For long-running handlers, `heartbeat_interval` renews the lease in the
background. It must be shorter than `lease_seconds`; one-third of the lease is
recommended. A handler can also renew it explicitly with
`job.extend_lease(seconds)`, using a positive duration.

## Event bus

Optional pub/sub on top of the same durable queues (requires the `bus` extra):

```bash
uv add "localqueue[bus]"
```

```python
# shared.py
from localqueue.bus import BaseEvent, BusTopology


class UserCreated(BaseEvent):
    event_name = "user.created"

    user_id: str


TOPOLOGY = BusTopology({"email": [UserCreated]})
```

```python
# producer.py
from localqueue.bus import EventBus

from shared import TOPOLOGY, UserCreated


bus = EventBus("./data", name="app", topology=TOPOLOGY)
bus.dispatch(UserCreated(user_id="123"))  # atomic fan-out, committed
bus.close()
```

```python
# email_worker.py
import asyncio

from localqueue.bus import EventBus

from shared import TOPOLOGY, UserCreated


bus = EventBus("./data", name="app", topology=TOPOLOGY)
email = bus.subscription("email")


@email.handler(UserCreated)
async def send_welcome(event: UserCreated) -> None: ...


asyncio.run(bus.run())  # consume subscriptions handled by this process
```

Each subscription is a durable queue (`__bus__:{bus}:{subscription}`), so
workers in multiple processes act as consumer groups. Handlers get the same
retry, lease, and dead-letter semantics as regular jobs. The static topology
decides where events are persisted; local handlers decide what the current
process consumes. Producers do not import handlers, and workers do not
participate in dispatch. See
[docs/event-bus.md](https://github.com/brunoportis/localqueue/blob/main/docs/event-bus.md).

## Delivery guarantees

> [!IMPORTANT]
> `localqueue` provides **at-least-once** delivery, not exactly-once delivery.
> A worker can finish an external side effect and crash before `ack()`, causing
> the job to be delivered again. Make handlers idempotent when duplicate effects
> matter.

```text
put() ──> ready ──> leased ──> acked
                     │
                     ├── nack() or expired lease ──> ready
                     │
                     └── fail() or retry limit ────> failed
                                                        │
                                           retry_failed() ──> ready
```

| Behavior | Guarantee |
| --- | --- |
| Successful `put()` | The job was committed to SQLite and has an internal ID. |
| Worker crash | An unacknowledged job becomes available after its lease expires. |
| Stale worker | `ack()`, `nack()`, `fail()`, and lease extension require the current receipt. |
| Retries | `max_retries` allows that many retries after the initial delivery; exhaustion moves the job to `failed`. |
| Deduplication | A `job_id` is unique within a named queue while its record exists. |
| Ordering | Ready jobs are claimed by insertion ID, but completion order is best effort under concurrency. |

By default, SQLite uses `synchronous=NORMAL`, which protects against normal
process crashes but may lose recent transactions after an operating-system or
power failure. Pass `fsync=True` to use `synchronous=FULL` for stronger
durability.

See [Delivery guarantees](https://github.com/brunoportis/localqueue/blob/main/docs/guarantees.md)
for the complete durability, lease, retry, fencing, and deduplication contract.

## Configuration

```python
queue = SimpleQueue(
    "./data",
    name="emails",
    lease_seconds=60,
    max_retries=3,
    fsync=False,
    serializer=None,
    max_pending_jobs=None,
)
```

| Option | Default | Description |
| --- | ---: | --- |
| `path` | required | Directory containing the shared `localqueue.db` file. |
| `name` | `"default"` | Logical queue name; several queues can share one database. |
| `lease_seconds` | `60.0` | Time a worker owns a delivery before it can be reclaimed. |
| `max_retries` | `3` | Retries allowed after the first delivery. |
| `fsync` | `False` | Use SQLite `synchronous=FULL` when enabled. |
| `serializer` | JSON | Object implementing `dumps(obj) -> bytes` and `loads(bytes) -> obj`. |
| `max_pending_jobs` | `None` | Optional positive limit for ready + processing jobs in this logical queue. |

## Bounded backlog and backpressure

Limit one logical queue by job count and choose whether producers wait:

```python
from localqueue import Full, SimpleQueue


queue = SimpleQueue("./data", max_pending_jobs=10_000)
try:
    queue.put(payload, block=False)
except Full:
    ...

queue.put(payload, timeout=5.0)
```

Pending means `ready + processing`: delayed jobs, active leases, and expired
unreclaimed leases count; ACKed and failed jobs do not. The native capacity
check, deduplication calculation, and complete insert share one
`BEGIN IMMEDIATE` transaction, so participating processes using the same
configured limit cannot oversubscribe it. `put_many()` is all or nothing, and
duplicates that do not create rows consume no new slots, even above the limit.
A batch whose deduplicated new-row count exceeds the limit raises `Full`
immediately because it can never fit.

This limits logical backlog, not SQLite/WAL bytes, disk space, or retained
terminal records. The setting belongs to each `SimpleQueue` object and is not
persisted: all producers for a logical queue must use the same limit, while an
unlimited or direct-SQL client can bypass the contract. EventBus fanout remains
unlimited. See the
[backpressure reference](https://github.com/brunoportis/localqueue/blob/main/docs/backpressure.md)
for polling, timeout, transition, multiprocess, and error details.

## API overview

| Method | Purpose |
| --- | --- |
| `put(data, job_id=None, *, block=True, timeout=None)` | Enqueue with optional deduplication and capacity waiting. |
| `put_many(items, *, block=True, timeout=None)` | Atomically enqueue a complete batch, optionally using `EnqueueItem` for per-item deduplication. |
| `get(block=True, timeout=None)` | Claim a job and start its lease. |
| `get_nowait()` | Claim immediately or raise `Empty`. |
| `ack(job)` | Confirm successful processing. |
| `nack(job, delay=0, last_error=None)` | Return a transient failure to the queue, optionally with a non-negative delay. |
| `fail(job, last_error=None)` | Move a permanent failure to the dead-letter state. |
| `extend_lease(job, seconds)` | Renew the current delivery lease by a positive duration. |
| `reclaim_expired_leases()` | Reclaim expired leases explicitly; `get()` also does this automatically. |
| `stats()` | Return `ready`, `processing`, `acked`, and `failed` counts. |
| `diagnostics()` | Return a typed, immutable, read-only operational snapshot. |
| `check_integrity(*, mode="full", max_errors=100)` | Run a bounded, typed full or quick SQLite integrity check. |
| `backup(destination_directory)` | Create a verified online backup in a new exclusive directory. |
| `list_failed(limit=100, offset=0)` | Inspect dead-letter jobs. |
| `retry_failed(message_id)` | Non-blockingly move a dead-letter job back to `ready`, or raise `Full`. |
| `purge(older_than, include_failed=False)` | Delete old terminal records. |
| `vacuum()` | Compact the shared SQLite database. Run during a maintenance window. |

## Runtime diagnostics

Capture a typed, immutable operational snapshot without opening SQLite
directly:

```python
import json

from localqueue import SimpleQueue


queue = SimpleQueue("./data")
report = queue.diagnostics()

print(report)
print(json.dumps(report.to_dict(), indent=2))
queue.close()
```

Diagnostics schema v2 reports logical counts, configured capacity, pending
jobs, available slots, and ages for the selected queue together
with effective SQLite settings and best-effort shared database/WAL/SHM sizes.
It is read-only and does not reclaim leases, checkpoint WAL, or run an
integrity check. See the
[runtime diagnostics reference](https://github.com/brunoportis/localqueue/blob/main/docs/diagnostics.md)
for field types, snapshot semantics, clock limitations, and the boundary with
integrity/backup maintenance.

## Integrity checks and online backups

Run explicit SQLite maintenance without reaching into the native connection:

```python
from localqueue import SimpleQueue


queue = SimpleQueue("./data")
integrity = queue.check_integrity()
if integrity.ok:
    backup = queue.backup("./backups/2026-07-22")
    print(backup.to_dict())
queue.close()
```

Backups use SQLite's Online Backup API and remain consistent while other
connections produce and consume jobs. The destination directory is reserved
exclusively and must not already exist. See the
[maintenance reference](https://github.com/brunoportis/localqueue/blob/main/docs/maintenance.md)
for result fields, locking and latency, filesystem requirements, error
semantics, independent verification, and recovery steps.

## Architecture

The public API is a small Python facade over a native
[PyO3](https://pyo3.rs/) extension. Rust owns the transactional state machine,
while SQLite WAL mode provides local persistence and coordinates competing
writers.

```text
Python application
       │
       ▼
SimpleQueue / Worker       Python API and serialization
       │
       ▼
Rust native extension      leases, receipts, retries, transactions
       │
       ▼
localqueue.db              SQLite WAL, one machine
```

All queue state lives in one SQLite table. Claims and multi-step transitions
use immediate transactions so concurrent processes cannot reserve the same
delivery. This is local infrastructure for a shared database on one machine,
not a distributed message broker.

## Benchmarks

The canonical single-process benchmark is run with the optional benchmark
extra:

```bash
uv sync --extra benchmark
uv run python -m localqueue.benchmark --profile smoke --output smoke.json
uv run python -m localqueue.benchmark --profile multiprocess-ci --output multiprocess.json
```

See the [benchmark guide](docs/benchmarks.md) for NORMAL/FULL durability and
report interpretation. Benchmark output is evidence about a particular host,
filesystem, and run; it is not a product performance promise.

## Development

Build the extension in a local virtual environment and run the test suite:

```bash
uv sync --extra dev
uv run maturin develop
uv run pytest
```

Run the configured strict type check for the production package:

```bash
uv run pyrefly check --progress-bar no
```

Rust quality checks:

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Build a release wheel:

```bash
uv run maturin build --release --locked
```

## License

Licensed under the [Apache License 2.0](https://github.com/brunoportis/localqueue/blob/main/LICENSE).
