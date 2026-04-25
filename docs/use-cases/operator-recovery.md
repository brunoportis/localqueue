---
icon: lucide/life-buoy
---

# Operator Recovery

Use this pattern when jobs can fail in recoverable ways and operators need to
inspect, dead-letter, and replay them from the terminal.

This is where `localqueue` feels more useful than a pile of shell scripts:
failures stay visible, recovery is explicit, and replay does not require a
custom one-off script.

## Best fit

- webhook delivery with occasional upstream outages
- notification pipelines that need human inspection on failure
- CLI-driven maintenance jobs where operators need a dead-letter queue

## Why `localqueue` fits

- failed jobs can move to dead-letter after a small retry budget
- operators can inspect summary and full records locally
- replay is one command instead of ad hoc recovery code

## Boundaries

- recovery is local to one machine and one shared store
- this does not replace broker-level operations across multiple services

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

Create a failing job:

```bash
echo '{"to":"broken@example.com","fail":true}' | localqueue queue add webhooks
```

Process it with a small retry budget so it reaches dead-letter quickly:

```bash
localqueue queue exec webhooks \
  --max-tries 2 \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Inspect the dead-letter summary and full record:

```bash
localqueue queue dead webhooks --summary

localqueue queue dead webhooks
```

Replay everything in the dead-letter queue:

```bash
localqueue queue requeue-dead webhooks --all
```

