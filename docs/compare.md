---
icon: lucide/inbox
---

# Compare

`localqueue` fits durable single-host workers. This page is a quick chooser
between that model and common brokered alternatives.

## Use `localqueue` when

- you want a local filesystem-backed queue
- jobs are handled by scripts, CLIs, or small Python workers
- you care more about operational simplicity than distributed scheduling
- at-least-once delivery is acceptable
- you want to inspect and requeue failed work from the terminal
- you want worker-side controls like `min_interval`, queue-level retry defaults,
  and a simple circuit breaker without introducing a broker

## Choose a brokered system when

- producers and consumers must run on different machines
- you need broker-managed delivery guarantees
- write contention is high
- retention, metrics, sharding, or fanout are first-class requirements
- you want a queue that is already part of a larger distributed platform
- you need broker-level rate limiting or cross-host worker coordination

## Quick comparison

| System | Best fit | Why choose it instead |
| --- | --- | --- |
| `localqueue` | single-host workers, scripts, local ops | simplest path when the queue can live on one filesystem |
| Celery | Python task orchestration with a broker | richer distributed worker ecosystem |
| RQ | Redis-backed Python jobs | simpler Redis-based queueing for server-side apps |
| Dramatiq | Python workers with broker support | clean task API for distributed workers |
| SQS | managed cloud queueing | external durability and multi-host coordination |
| RabbitMQ | brokered messaging | mature routing and delivery primitives |
| Redis Streams | stream processing and consumer groups | shared stream coordination with Redis |
| Kafka | high-throughput event pipelines | log-based distributed ingestion and replay |

## Rule of thumb

If the queue can stay local and the value is in simple recovery, `localqueue` fits.
If the queue itself is part of a distributed system boundary, use a broker or stream platform.

See also:

- [Operations guide](operational-maturity.md)
- [Persistent queues](queues.md)
- [Persistent retries](retries.md)
