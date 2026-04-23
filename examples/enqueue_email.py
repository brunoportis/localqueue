from __future__ import annotations

import argparse
from typing import Any

from localqueue import PersistentQueue


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--store-path")
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()

    queue: PersistentQueue[dict[str, Any]] = (
        PersistentQueue("emails", store_path=args.store_path)
        if args.store_path is not None
        else PersistentQueue("emails")
    )
    message = queue.put({"to": args.address, "fail": args.fail})
    print(message.id)


if __name__ == "__main__":
    main()
