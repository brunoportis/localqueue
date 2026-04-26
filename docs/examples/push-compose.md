---
icon: lucide/arrow-up-from-line
---

# Docker Compose push example

This example shows local push-style dispatch in one container.

`localqueue` persists the message, then `CallbackDispatcher` handles it in the
same process. This is not cross-container push.

## Run it

From the repo root:

```bash
cd examples/docker-compose/push
docker compose down -v
docker compose up --build
```

Expected result:

- the container exits with code `0`
- both messages are dispatched immediately
- final queue stats show `total: 0`

Representative output. Message IDs may differ:

```json
{"semantics":{"locality":"local","delivery":"at-least-once","routing":"point-to-point","consumption":"push","ordering":"fifo-ready","leases":true,"acknowledgements":true,"dead_letters":true,"deduplication":true,"subscriptions":false,"notifications":false}}
{"event":"dispatch","message_id":"411a356bc9d34499873cbd21ecef8f9f","payload":{"kind":"signup","user_id":"user-1"}}
{"event":"enqueue","message_id":"411a356bc9d34499873cbd21ecef8f9f"}
{"event":"dispatch","message_id":"106f3362e70d4854a341689e7c7a2793","payload":{"kind":"invoice-paid","user_id":"user-2"}}
{"event":"enqueue","message_id":"106f3362e70d4854a341689e7c7a2793"}
{"processed_ids":["411a356bc9d34499873cbd21ecef8f9f","106f3362e70d4854a341689e7c7a2793"],"stats":{"ready":0,"delayed":0,"inflight":0,"dead":0,"total":0,"by_worker_id":{},"leases_by_worker_id":{},"last_seen_by_worker_id":{},"oldest_ready_age_seconds":null,"oldest_inflight_age_seconds":null,"average_inflight_age_seconds":null}}
```

## Why this works

- `examples/docker-compose/push/compose.yaml` mounts a writable Docker volume at `/data`
- the service runs as `root` so SQLite can create `/data/queue.sqlite3`
- `examples/docker-compose/push/push_app.py` acknowledges each dispatched message

## Cleanup

```bash
docker compose down -v
```
