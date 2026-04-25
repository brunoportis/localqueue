---
icon: lucide/mail
---

# Local Outbox

Use this pattern when the caller should return now and the external side effect
can happen a few seconds later on the same machine.

This is one of the strongest `localqueue` stories: accept work quickly, store
it durably, and let a local worker perform the slow or fragile side effect
later.

## Best fit

- email delivery from a web app or admin script
- webhook fan-out from a local integration process
- uploads, report generation, or notifications that should not block the caller

## Why `localqueue` fits

- no broker, Redis, or extra service to operate
- queue state survives process restarts
- the worker can be a Python function or an external command

## Boundaries

- this is a single-host pattern, not a distributed outbox
- delivery is at least once, so handlers should be idempotent

## Setup

Create a scratch directory and a small worker script once:

```python
from __future__ import annotations

import json
import sys


payload = json.load(sys.stdin)
address = payload["to"]
if payload.get("fail"):
    raise ConnectionError(f"could not deliver email to {address}")
print(f"sent email to {address}")
```

## Minimal flow

Enqueue one email job:

```bash
echo '{"to":"user@example.com"}' | localqueue queue add emails
```

Inspect the queue before processing:

```bash
localqueue queue stats emails
```

Process one queued message with the worker script:

```bash
localqueue queue exec emails \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Check that the queue is empty again:

```bash
localqueue queue stats emails
```

