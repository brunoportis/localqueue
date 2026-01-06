from __future__ import annotations

import argparse

from persistentqueue import PersistentQueue


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--store-path", default="./persistence_db")
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()

    queue = PersistentQueue("emails", store_path=args.store_path)
    message = queue.put({"to": args.address, "fail": args.fail})
    print(message.id)


if __name__ == "__main__":
    main()
