# Getting started

Install the package:

```bash
uv add localqueue
```

The queue path is a directory. `localqueue` creates and manages
`localqueue.db` inside it.

## 1. Produce a durable job

Create `producer.py`:

```python
from localqueue import SimpleQueue


with SimpleQueue("./data", lease_seconds=30, max_retries=3) as queue:
    queue.put(
        {"task": "send-email", "to": "hello@example.com"},
        job_id="welcome-email-42",
    )
```

Run it once:

```bash
python producer.py
```

The producer can now exit. The job has been committed to
`./data/localqueue.db` and remains available to another process.

## 2. Consume it later

Create `worker.py`:

```python
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

Run it in a later shell or after restarting the first process:

```bash
python worker.py
```

`ack()` marks the job complete. If handling raises an exception, `nack()` makes
it eligible for retry; after the retry limit it moves to the dead-letter state.

## Durability settings

The default SQLite setting protects committed jobs from ordinary process crashes.
For stronger protection against operating-system or power failure, create the
queue with `fsync=True`:

```python
SimpleQueue("./data", fsync=True)
```

See [Delivery guarantees](guarantees.md) for the full lease, retry,
deduplication, ordering, and durability contract.

To bound logical backlog, pass a positive `max_pending_jobs`. Pending is the
sum of ready and processing jobs, not database bytes. See
[Bounded backlog and producer backpressure](backpressure.md).
