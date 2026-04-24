---
icon: lucide/inbox
---

# Overview

`localqueue` is a small durable queue for one machine. It stores work on the local filesystem by default and keeps retry state with Tenacity.

It fits scripts, CLI tools, cron jobs, development helpers, and small workers that can safely share one local store. It is not a distributed broker and does not provide multi-host coordination.

!!! note
    The default model is local-file storage, at-least-once delivery, and best-effort ordering under concurrency.

## CLI

Use the CLI when you want to enqueue, inspect, watch, and recover jobs from the terminal.

```bash
localqueue queue add emails --value '{"to":"user@example.com"}'
localqueue queue exec emails -- python scripts/send_email.py
localqueue queue stats emails --watch --interval 1
localqueue queue dead emails --summary
localqueue queue health emails
```

`queue exec` runs an external command per message. The message value is written to stdin as JSON, and exit code `0` acks the message.

For more command examples, see [Persistent queues](queues.md).

## Library

Use the Python API when the worker lives in your codebase.

```python
from localqueue import PersistentQueue, persistent_worker

queue = PersistentQueue("emails")
queue.put({"to": "user@example.com"})

@persistent_worker(queue)
def send_email(job: dict[str, str]) -> None:
    deliver(job["to"])
```

`localqueue.retry` stays available when another system already delivers work and you only need retry state.

See [Persistent retries](retries.md) for the retry API and store options.

## When to use

- small Python workers on one machine
- scripts that need durable work after restarts
- terminal-driven queues for jobs you want to inspect and requeue
- retry state that must survive process restarts

## When not to use

- multi-host scheduling
- high write concurrency
- managed retention or broker-level metrics
- strict global ordering
- distributed coordination

See [Compare](compare.md) for a short decision guide.

## Read more

- [Persistent queues](queues.md)
- [Persistent retries](retries.md)
- [Stability](stability.md)
- [Compare](compare.md)
- [API reference](api.md)
- [Operational maturity](operational-maturity.md)
- [Development guide](develop.md)
- [Release checklist](release.md)
