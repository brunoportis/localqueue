# Durable local jobs, without another service

`localqueue` is a persistent queue for Python processes running on the same
machine. It stores jobs in SQLite and uses a Rust extension for the
transactional queue state machine.

```text
producer process               SQLite database                 worker process
       │                              │                                │
       ├── put(job) ────────────────► │                                │
       │                              │ ◄── get() / ACK or NACK ───────┤
       └── may exit safely            │                                │
```

It is useful when a script, web process, or local service needs durable work
without operating Redis, RabbitMQ, or another network service.

## What it gives you

- **Durable jobs:** a successful `put()` commits to `localqueue.db`; a later
  process can reopen the same directory and consume the job.
- **Multiprocess safety:** workers on the same machine coordinate through the
  shared SQLite database.
- **Failure handling:** leases, retries, dead-letter jobs, and receipt fencing
  are part of the queue contract.
- **Optional Event Bus:** static topology, atomic fan-out, and consumer groups
  on top of the same durable queues.

## Start here

```bash
uv add localqueue
```

Run the [two-process quickstart](getting-started.md) to see a producer exit
after writing a job and a later worker consume it.

!!! important "Delivery model"

    `localqueue` provides **at-least-once** delivery. Handlers that perform
    external side effects should be idempotent, because a process can fail after
    completing work and before acknowledging it.

## What it is not

`localqueue` is not a network broker, distributed queue, or Kafka replacement.
It coordinates processes that can access the same local filesystem and SQLite
database. It does not provide partitions, retention, replay, or cross-machine
transport.

Read [Delivery guarantees](guarantees.md) before choosing operational settings,
especially when crash recovery and power-loss durability matter.
