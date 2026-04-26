# Pull example

This example shows `localqueue` in its default pull model:

- `producer` container enqueues email jobs into SQLite under `/data`
- `worker` container runs `localqueue queue exec` and processes jobs from the same volume

## Run

Build image and enqueue sample jobs:

```bash
docker compose up --build producer
```

Start worker loop in separate terminal:

```bash
docker compose up worker
```

Stop worker with `Ctrl-C` after it drains queue.

## What it demonstrates

- pull-based delivery across containers
- shared durable queue state with named volume
- command worker usage with `queue exec`
