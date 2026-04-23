#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import threading
import time
from collections import Counter
from collections.abc import Callable
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

    _remove_sqlite_files(store_path)

    start = threading.Event()
    producers_done = threading.Event()
    consumed = Counter[str]()
    consumed_lock = threading.Lock()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    lock_retries = Counter[str]()
    per_producer, remainder = divmod(messages, producers)

    producer_threads = [
        threading.Thread(
            target=_producer_thread,
            args=(
                index,
                queue_name,
                store_path,
                lease_timeout,
                start,
                per_producer + (1 if index < remainder else 0),
                lock_retries,
                errors,
                errors_lock,
            ),
        )
        for index in range(producers)
    ]
    consumer_threads = [
        threading.Thread(
            target=_consumer_thread,
            args=(
                index,
                queue_name,
                store_path,
                lease_timeout,
                start,
                producers_done,
                consumed,
                consumed_lock,
                messages,
                lock_retries,
                errors,
                errors_lock,
            ),
        )
        for index in range(consumers)
    ]
    threads = producer_threads + consumer_threads

    started_at = time.perf_counter()
    _start_threads(threads)
    start.set()

    _join_benchmark_threads(
        threads,
        producer_count=producers,
        producers_done=producers_done,
        errors=errors,
        errors_lock=errors_lock,
    )
    elapsed = time.perf_counter() - started_at
    return _benchmark_summary(
        queue_name=queue_name,
        store_path=store_path,
        lease_timeout=lease_timeout,
        messages=messages,
        producers=producers,
        consumers=consumers,
        consumed=consumed,
        errors=errors,
        lock_retries=lock_retries,
        elapsed=elapsed,
    )


def _remove_sqlite_files(store_path: str) -> None:
    store_file = Path(store_path)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{store_file}{suffix}")
        if candidate.exists():
            candidate.unlink()


def _record_error(
    exc: BaseException, errors: list[BaseException], errors_lock: threading.Lock
) -> None:
    with errors_lock:
        errors.append(exc)


def _retry_sqlite_locked(
    action: str, fn: Callable[[], Any], lock_retries: Counter[str]
) -> Any:
    attempts = 0
    while True:
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower():
                raise
            attempts += 1
            lock_retries[action] += 1
            time.sleep(min(0.001 * attempts, 0.05))


def _producer_thread(
    index: int,
    queue_name: str,
    store_path: str,
    lease_timeout: float,
    start: threading.Event,
    message_count: int,
    lock_retries: Counter[str],
    errors: list[BaseException],
    errors_lock: threading.Lock,
) -> None:
    try:
        queue: PersistentQueue[dict[str, Any]] = PersistentQueue(
            queue_name, store_path=store_path, lease_timeout=lease_timeout
        )
        start.wait()
        for sequence in range(message_count):
            _retry_sqlite_locked(
                "put",
                lambda sequence=sequence: queue.put(
                    {"producer": index, "sequence": sequence}
                ),
                lock_retries,
            )
    except Exception as exc:  # pragma: no cover - defensive
        _record_error(exc, errors, errors_lock)


def _consumer_thread(
    index: int,
    queue_name: str,
    store_path: str,
    lease_timeout: float,
    start: threading.Event,
    producers_done: threading.Event,
    consumed: Counter[str],
    consumed_lock: threading.Lock,
    messages: int,
    lock_retries: Counter[str],
    errors: list[BaseException],
    errors_lock: threading.Lock,
) -> None:
    try:
        queue: PersistentQueue[dict[str, Any]] = PersistentQueue(
            queue_name, store_path=store_path, lease_timeout=lease_timeout
        )
        start.wait()
        while not _all_messages_consumed(
            producers_done, consumed, consumed_lock, messages
        ):
            try:
                message = _retry_sqlite_locked(
                    "get",
                    lambda: queue.get_message(
                        block=False, leased_by=f"consumer-{index}"
                    ),
                    lock_retries,
                )
            except Empty:
                time.sleep(0.001)
                continue
            if not _retry_sqlite_locked(
                "ack", lambda message=message: queue.ack(message), lock_retries
            ):
                _record_error(
                    RuntimeError(f"ack failed for {message.id}"), errors, errors_lock
                )
                return
            with consumed_lock:
                consumed[message.id] += 1
    except Exception as exc:  # pragma: no cover - defensive
        _record_error(exc, errors, errors_lock)


def _all_messages_consumed(
    producers_done: threading.Event,
    consumed: Counter[str],
    consumed_lock: threading.Lock,
    messages: int,
) -> bool:
    with consumed_lock:
        return producers_done.is_set() and sum(consumed.values()) >= messages


def _start_threads(threads: list[threading.Thread]) -> None:
    for thread in threads:
        thread.start()


def _join_benchmark_threads(
    threads: list[threading.Thread],
    *,
    producer_count: int,
    producers_done: threading.Event,
    errors: list[BaseException],
    errors_lock: threading.Lock,
) -> None:
    for thread in threads[:producer_count]:
        thread.join()
    producers_done.set()

    for thread in threads[producer_count:]:
        thread.join(timeout=10.0)
        if thread.is_alive():
            _record_error(
                TimeoutError("consumer thread did not finish"), errors, errors_lock
            )


def _benchmark_summary(
    *,
    queue_name: str,
    store_path: str,
    lease_timeout: float,
    messages: int,
    producers: int,
    consumers: int,
    consumed: Counter[str],
    errors: list[BaseException],
    lock_retries: Counter[str],
    elapsed: float,
) -> dict[str, Any]:
    queue: PersistentQueue[dict[str, Any]] = PersistentQueue(
        queue_name, store_path=store_path, lease_timeout=lease_timeout
    )
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
        "lock_retries": dict(lock_retries),
        "elapsed_seconds": round(elapsed, 6),
        "throughput_messages_per_second": round(throughput, 2),
        "stats": stats,
    }


if __name__ == "__main__":
    raise SystemExit(main())
