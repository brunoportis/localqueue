from __future__ import annotations

import asyncio
import json
import os

from websockets.asyncio.client import connect


WS_URL = os.environ.get("WS_URL", "ws://app:8765")
EXPECTED_MESSAGES = int(os.environ.get("EXPECTED_MESSAGES", "2"))


async def main() -> None:
    received = 0
    async with connect(WS_URL) as websocket:
        async for message in websocket:
            print(message)
            received += 1
            if received >= EXPECTED_MESSAGES:
                break

    print(json.dumps({"event": "client_complete", "received": received}))


if __name__ == "__main__":
    asyncio.run(main())
