---
icon: lucide/radio-tower
---

# Docker Compose WebSocket example

This example shows `localqueue` projecting persisted enqueue events to a
WebSocket client.

- `app` stores messages in SQLite on a shared Docker volume
- `app` uses `WebSocketNotification` to send JSON frames to the connected client
- `client` prints each WebSocket frame, then exits

Important: the WebSocket adapter is a notification path, not a worker. The
messages remain in the queue after the notifications are sent.

## Run it

From the repo root:

```bash
cd examples/docker-compose/websocket
docker compose down -v
docker compose up --build
```

Expected result:

- `client` exits with code `0`
- the client prints two `queue.notification` frames
- final queue stats show `ready: 2` and `total: 2`

Representative output. Message IDs may differ:

```json
{"event":"server_ready","ws_url":"ws://0.0.0.0:8765"}
{"event":"client_connected"}
{"semantics":{"locality":"local","delivery":"at-least-once","routing":"point-to-point","consumption":"push","ordering":"fifo-ready","leases":true,"acknowledgements":true,"dead_letters":true,"deduplication":true,"subscriptions":false,"notifications":true}}
{"event":"enqueue","message_id":"60af98624e244febb8553c3520ab99a4"}
{"event":"enqueue","message_id":"e514d84d7ca2431f8aaee7531ce7bf40"}
{"event":"queue.notification","message_id":"60af98624e244febb8553c3520ab99a4","queue":"websocket-events","payload":{"kind":"signup","user_id":"user-1"},"state":"ready","attempts":0}
{"event":"queue.notification","message_id":"e514d84d7ca2431f8aaee7531ce7bf40","queue":"websocket-events","payload":{"kind":"invoice-paid","user_id":"user-2"},"state":"ready","attempts":0}
{"event":"client_complete","received":2}
{"stats":{"ready":2,"delayed":0,"inflight":0,"dead":0,"total":2,"by_worker_id":{},"leases_by_worker_id":{},"last_seen_by_worker_id":{},"oldest_ready_age_seconds":0.0006,"oldest_inflight_age_seconds":null,"average_inflight_age_seconds":null}}
```

## Why this works

- `examples/docker-compose/websocket/Dockerfile` installs `websockets` on top of the repo package
- `examples/docker-compose/websocket/compose.yaml` runs app and client in one networked Compose stack
- `examples/docker-compose/websocket/websocket_app.py` attaches `WebSocketNotification` after persistence
- queue stats stay non-zero because notification does not acknowledge messages

## Cleanup

```bash
docker compose down -v
```
