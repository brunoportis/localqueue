# WebSocket example

This example shows `localqueue` projecting enqueue notifications to a WebSocket
client in one Compose stack.

- `app` persists messages into SQLite under `/data`
- `app` publishes enqueue notifications through `WebSocketNotification`
- `client` connects over WebSocket and prints the JSON frames it receives

Important: this is notification, not queue consumption. The messages remain in
the durable queue after they are sent to the WebSocket client.

## Run

```bash
docker compose down -v
docker compose up --build
```

## What it demonstrates

- async WebSocket projection after persistence
- `PushConsumption` plus `WebSocketNotification`
- queue state remains durable after notification
