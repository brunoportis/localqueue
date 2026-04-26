from __future__ import annotations

import json
import os

from localqueue import PersistentQueue


QUEUE_NAME = os.environ.get("QUEUE_NAME", "emails")
QUEUE_STORE_PATH = os.environ.get("QUEUE_STORE_PATH", "/data/queue.sqlite3")


def main() -> None:
    queue: PersistentQueue[dict[str, str]] = PersistentQueue(
        QUEUE_NAME,
        store_path=QUEUE_STORE_PATH,
    )

    jobs = [
        {"to": "alice@example.com", "subject": "welcome"},
        {"to": "bob@example.com", "subject": "invoice-ready"},
        {"to": "carol@example.com", "subject": "weekly-report"},
    ]

    for job in jobs:
        message = queue.put(job)
        print(json.dumps({"message_id": message.id, "payload": job}))

    print(json.dumps({"queue": QUEUE_NAME, "stats": queue.stats().as_dict()}))


if __name__ == "__main__":
    main()
