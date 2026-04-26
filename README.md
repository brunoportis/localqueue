# `localqueue`

[![Tests](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml)
[![CodeQL](https://github.com/brunoportis/localqueue/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/github-code-scanning/codeql)
[![PyPI version](https://img.shields.io/pypi/v/localqueue.svg)](https://pypi.org/project/localqueue/)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

`localqueue` is a small durable queue for one machine. It stores work on the
local filesystem by default and keeps retry state with Tenacity.

Use it when the work can stay local and you want one of these outcomes:

- accept work now and perform the side effect later on the same machine
- inspect failed jobs and replay them from the terminal
- keep retry budgets across process restarts without adding a broker

It fits scripts, CLI tools, cron jobs, and small Python workers that share one
local store.

```bash
localqueue queue exec emails -- python scripts/send_email.py
```

```python
from localqueue import PersistentQueue, persistent_worker

queue = PersistentQueue("emails")
queue.put({"to": "user@example.com"})

@persistent_worker(queue)
def send_email(job: dict[str, str]) -> None:
    deliver(job["to"])
```

If you prefer a fluent builder that keeps queue defaults and worker defaults in
one place, use `QueueSpec` and build each runtime object explicitly:

```python
from localqueue import QoS, QueueSpec

spec = (
    QueueSpec("orders.payment")
    .with_qos(QoS.AT_LEAST_ONCE)
    .with_retry(max_retries=2)
    .with_dead_letter_on_failure(False)
    .with_release_delay(5.0)
    .with_circuit_breaker(threshold=3, cooldown=30.0)
    .with_dead_letter_queue()
)

queue = spec.build_queue()
worker_config = spec.build_worker_config()
```

For local publish/subscribe fanout, configure subscribers explicitly and
consume each physical subscriber queue independently:

```python
from localqueue import (
    PersistentQueue,
    PublishSubscribeRouting,
    StaticFanoutSubscriptions,
)

events = PersistentQueue(
    "events",
    routing_policy=PublishSubscribeRouting(),
    subscription_policy=StaticFanoutSubscriptions(("billing", "audit")),
)

events.put({"kind": "invoice-paid", "invoice_id": "inv-1"})

billing = events.subscriber_queue("billing")
audit = events.subscriber_queue("audit")

billing_message = billing.get_message()
audit_message = audit.get_message()
```

Full docs live at [brunoportis.github.io/localqueue](https://brunoportis.github.io/localqueue/). The source docs are in [`docs/`](docs/).

Container examples live in [`examples/docker-compose/`](examples/docker-compose/README.md).
Step-by-step reproducible docs for them live in [`docs/examples.md`](docs/examples.md).

Start with [`docs/use-cases.md`](docs/use-cases.md) if you want the shortest
path to deciding whether the project fits your workflow.

For development and release workflow details, see [`docs/develop.md`](docs/develop.md).

## Install

```bash
pip install localqueue
```

For the CLI:

```bash
pip install "localqueue[cli]"
```

For the optional LMDB backend:

```bash
pip install "localqueue[lmdb]"
```

## Running with Docker

The image is also published on GitHub Container Registry:

```bash
docker pull ghcr.io/brunoportis/localqueue:latest
docker run --rm ghcr.io/brunoportis/localqueue:latest --help
```

## When not to use

`localqueue` is not a distributed broker. If you need multi-host coordination, high write concurrency, managed retention, or strict cross-service ordering, use a system designed for that operating model.
