from __future__ import annotations

from typing import Any


def send_email(payload: dict[str, Any]) -> None:
    address = str(payload["to"])
    if payload.get("fail"):
        raise ConnectionError(f"could not deliver email to {address}")
    print(f"sent email to {address}")
