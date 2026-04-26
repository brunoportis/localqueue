from __future__ import annotations

import json
import sys


def main() -> None:
    payload = json.load(sys.stdin)
    address = payload["to"]
    subject = payload["subject"]
    print(f"sent {subject} email to {address}")


if __name__ == "__main__":
    main()
