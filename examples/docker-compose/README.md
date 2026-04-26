# Docker Compose examples

This directory has three small container examples for `localqueue`.

- [`pull/`](pull/README.md): producer and worker in separate containers, sharing one durable queue store through a volume.
- [`push/`](push/README.md): one app container using `PushConsumption` plus `CallbackDispatcher` for local in-process dispatch.
- [`websocket/`](websocket/README.md): app and client containers showing `WebSocketNotification` sending enqueue events over a WebSocket connection.

The pull and push examples build directly from the repo `Dockerfile`. The
WebSocket example uses its own small `Dockerfile` so it can add the
`websockets` dependency without changing the base package image.
