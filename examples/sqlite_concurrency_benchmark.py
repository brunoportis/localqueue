#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import threading
import time
from collections import Counter
from pathlib import Path
from queue import Empty
from typing import Any

from localqueue import PersistentQueue


def main() -> int:
    args = _parse_args()
    summary = _run_benchmark(
        queue_name=args.queue,
        store_path=args.store_path,
        producers=args.producers,
        consumers=args.consumers,
        messages=args.messages,
        lease_timeout=args.lease_timeout,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["errors"] == 0 else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure concurrent SQLite queue throughput locally."
    )
    parser.add_argument("--queue", default="jobs")
    parser.add_argument(
        "--store-path",
        default="localqueue_queue_benchmark.sqlite3",
        help="SQLite queue file to use for the benchmark.",
    )
    parser.add_argument("--messages", type=int, default=200)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=4)
    parser.add_argument("--lease-timeout", type=float, default=0.2)
    return parser.parse_args()


def _run_benchmark(
    *,
    queue_name: str,
    store_path: str,
    producers: int,
    consumers: int,
    messages: int,
    lease_timeout: float,
) -> dict[str, Any]:
    if producers <= 0 or consumers <= 0 or messages <= 0:
        raise ValueError("producers, consumers, and messages must be greater than zero")

    start = threading.Event()
    producers_done = threading.Event()
    consumed = Counter[str]()
    consumed_lock = threading.Lock()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    per_producer, remainder = divmod(messages, producers)

    def record_error(exc: BaseException) -> None:
        with errors_lock:
            errors.append(exc)

    def producer(index: int) -> None:
        try:
            queue = PersistentQueue(
                queue_name, store_path=store_path, lease_timeout=lease_timeout
            )
            start.wait()
            count = per_producer + (1 if index < remainder else 0)
            for sequence in range(count):
                queue.put({"producer": index, "sequence": sequence})
        except BaseException as exc:  # pragma: no cover - defensive
            record_error(exc)

    def consumer(index: int) -> None:
        try:
            queue = PersistentQueue(
                queue_name, store_path=store_path, lease_timeout=lease_timeout
            )
            start.wait()
            while True:
                with consumed_lock:
                    if producers_done.is_set() and sum(consumed.values()) >= messages:
                        return
                try:
                    message = queue.get_message(
                        block=False, leased_by=f"consumer-{index}"
                    )
                except Empty:
                    if producers_done.is_set():
                        with consumed_lock:
                            if sum(consumed.values()) >= messages:
                                return
                    time.sleep(0.001)
                    continue
                if not queue.ack(message):
                    record_error(RuntimeError(f"ack failed for {message.id}"))
                    return
                with consumed_lock:
                    consumed[message.id] += 1
        except BaseException as exc:  # pragma: no cover - defensive
            record_error(exc)

    threads = [
        threading.Thread(target=producer, args=(index,))
        for index in range(producers)
    ] + [
        threading.Thread(target=consumer, args=(index,))
        for index in range(consumers)
    ]

    started_at = time.perf_counter()
    for thread in threads:
        thread.start()
    start.set()

    for thread in threads[:producers]:
        thread.join()
    producers_done.set()

    for thread in threads[producers:]:
        thread.join(timeout=10.0)
        if thread.is_alive():
            record_error(TimeoutError("consumer thread did not finish"))

    elapsed = time.perf_counter() - started_at
    queue = PersistentQueue(queue_name, store_path=store_path, lease_timeout=lease_timeout)
    stats = queue.stats().as_dict()
    consumed_total = sum(consumed.values())
    throughput = consumed_total / elapsed if elapsed > 0 else 0.0
    return {
        "queue": queue_name,
        "store_path": str(Path(store_path).resolve()),
        "messages": messages,
        "producers": producers,
        "consumers": consumers,
        "consumed": consumed_total,
        "unique_messages": len(consumed),
        "errors": len(errors),
        "error_messages": [f"{type(error).__name__}: {error}" for error in errors],
        "elapsed_seconds": round(elapsed, 6),
        "throughput_messages_per_second": round(throughput, 2),
        "stats": stats,
    }


if __name__ == "__main__":
    raise SystemExit(main())
