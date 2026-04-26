---
icon: lucide/arrow-down-to-line
---

# Docker Compose pull example

This example shows default pull-based delivery across containers.

- `producer` enqueues jobs into a shared SQLite queue store
- `worker` consumes jobs from the same Docker volume

## Run it

From the repo root:

```bash
cd examples/docker-compose/pull
docker compose down -v
docker compose up --build producer
docker compose run --rm worker queue exec emails --store-path /data/queue.sqlite3 --retry-store-path /data/retries.sqlite3 -- python /examples/worker.py
docker compose run --rm worker queue exec emails --store-path /data/queue.sqlite3 --retry-store-path /data/retries.sqlite3 -- python /examples/worker.py
docker compose run --rm worker queue exec emails --store-path /data/queue.sqlite3 --retry-store-path /data/retries.sqlite3 -- python /examples/worker.py
docker compose run --rm worker queue stats emails --store-path /data/queue.sqlite3
```

Expected result:

- `producer` exits with code `0`
- each worker run returns one JSON record with `"state": "acked"`
- final queue stats show `ready: 0` and `total: 0`

Representative output. Message IDs and age counters may differ:

```json
{"message_id":"a985139f23f54d3f842115db0ed73909","payload":{"to":"alice@example.com","subject":"welcome"}}
{"message_id":"5c2ac86b6115479cad0b6b6f4dd6bae1","payload":{"to":"bob@example.com","subject":"invoice-ready"}}
{"message_id":"ea98fb1763f144f4b41d4d4a9c40d67e","payload":{"to":"carol@example.com","subject":"weekly-report"}}
{"queue":"emails","stats":{"ready":3,"delayed":0,"inflight":0,"dead":0,"total":3,"by_worker_id":{},"leases_by_worker_id":{},"last_seen_by_worker_id":{},"oldest_ready_age_seconds":0.0004954338073730469,"oldest_inflight_age_seconds":null,"average_inflight_age_seconds":null}}
{"id":"ea98fb1763f144f4b41d4d4a9c40d67e","state":"acked"}
{"id":"5c2ac86b6115479cad0b6b6f4dd6bae1","state":"acked"}
{"id":"a985139f23f54d3f842115db0ed73909","state":"acked"}
{"average_inflight_age_seconds":null,"by_worker_id":{},"dead":0,"delayed":0,"inflight":0,"last_seen_by_worker_id":{},"leases_by_worker_id":{},"oldest_inflight_age_seconds":null,"oldest_ready_age_seconds":null,"ready":0,"total":0}
```

## Why this works

- `examples/docker-compose/pull/compose.yaml` mounts one named volume at `/data`
- both services run as `root` so SQLite and retry-store files can be created in that volume
- the worker command uses an explicit retry-store path instead of relying on CLI defaults

If you want a long-running worker instead, the same service definition supports:

```bash
docker compose up worker
```

For a fully deterministic walkthrough, prefer the one-shot `docker compose run --rm worker ...` commands above.

## Cleanup

```bash
docker compose down -v
```
