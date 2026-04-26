from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from websockets.asyncio.server import ServerConnection, serve

from localqueue import PersistentQueue, PushConsumption, WebSocketNotification


QUEUE_NAME = os.environ.get("QUEUE_NAME", "websocket-events")
QUEUE_STORE_PATH = os.environ.get("QUEUE_STORE_PATH", "/data/queue.sqlite3")
WS_HOST = os.environ.get("WS_HOST", "0.0.0.0")
WS_PORT = int(os.environ.get("WS_PORT", "8765"))


def _serialize_message(message: Any) -> str:
    return json.dumps(
        {
            "event": "queue.notification",
            "message_id": message.id,
            "queue": message.queue,
            "payload": message.value,
            "state": message.state,
            "attempts": message.attempts,
        }
    )


async def handle_client(
    websocket: ServerConnection, stop_event: asyncio.Event
) -> None:
    queue = PersistentQueue(
        QUEUE_NAME,
        store_path=QUEUE_STORE_PATH,
        consumption_policy=PushConsumption(),
        notification_policy=WebSocketNotification(
            websocket, serializer=_serialize_message
        ),
    )

    print(json.dumps({"event": "client_connected"}))
    print(json.dumps({"semantics": queue.semantics.as_dict()}))

    for payload in (
        {"kind": "signup", "user_id": "user-1"},
        {"kind": "invoice-paid", "user_id": "user-2"},
    ):
        message = queue.put(payload)
        print(json.dumps({"event": "enqueue", "message_id": message.id}))

    await asyncio.sleep(0.1)
    print(json.dumps({"stats": queue.stats().as_dict()}))
    await websocket.close(code=1000, reason="demo complete")
    stop_event.set()


async def main() -> None:
    stop_event = asyncio.Event()

    async with serve(
        lambda websocket: handle_client(websocket, stop_event), WS_HOST, WS_PORT
    ):
        print(
            json.dumps(
                {"event": "server_ready", "ws_url": f"ws://{WS_HOST}:{WS_PORT}"}
            )
        )
        await stop_event.wait()


if __name__ == "__main__":
    asyncio.run(main())
