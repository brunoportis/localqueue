from __future__ import annotations

import json
import os

from localqueue import (
    CallbackDispatcher,
    PersistentQueue,
    PushConsumption,
    QueueMessage,
)


QUEUE_NAME = os.environ.get("QUEUE_NAME", "push-events")
QUEUE_STORE_PATH = os.environ.get("QUEUE_STORE_PATH", "/data/queue.sqlite3")


def main() -> None:
    processed_ids: list[str] = []
    queue: PersistentQueue[dict[str, str]]

    def handle(message: QueueMessage) -> None:
        payload = message.value
        print(
            json.dumps(
                {
                    "event": "dispatch",
                    "message_id": message.id,
                    "payload": payload,
                }
            )
        )
        _ = queue.ack(message)
        processed_ids.append(message.id)

    queue = PersistentQueue(
        QUEUE_NAME,
        store_path=QUEUE_STORE_PATH,
        consumption_policy=PushConsumption(),
        dispatch_policy=CallbackDispatcher((handle,)),
    )

    print(json.dumps({"semantics": queue.semantics.as_dict()}))

    for payload in (
        {"kind": "signup", "user_id": "user-1"},
        {"kind": "invoice-paid", "user_id": "user-2"},
    ):
        message = queue.put(payload)
        print(json.dumps({"event": "enqueue", "message_id": message.id}))

    print(
        json.dumps(
            {
                "processed_ids": processed_ids,
                "stats": queue.stats().as_dict(),
            }
        )
    )


if __name__ == "__main__":
    main()
