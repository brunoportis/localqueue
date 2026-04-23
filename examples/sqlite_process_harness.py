#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path
from queue import Empty
from typing import Any, TYPE_CHECKING

from localqueue import PersistentQueue

if TYPE_CHECKING:
    from collections.abc import Sequence


def main() -> int:
    args = _parse_args()
    if args.mode == "throughput":
        summary = _run_throughput(
            queue_name=args.queue,
            store_path=args.store_path,
            producers=args.producers,
            consumers=args.consumers,
            messages=args.messages,
            lease_timeout=args.lease_timeout,
        )
    else:
        summary = _run_crash_recovery(
            queue_name=args.queue,
            store_path=args.store_path,
            messages=args.messages,
            lease_timeout=args.lease_timeout,
        )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["invariants_ok"] and summary["errors"] == 0 else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local SQLite queue process stress and chaos checks."
    )
    parser.add_argument(
        "--mode",
        choices=("throughput", "crash-recovery"),
        default="throughput",
        help="Scenario to run.",
    )
    parser.add_argument("--queue", default="jobs")
    parser.add_argument(
        "--store-path",
        default="localqueue_queue_process_harness.sqlite3",
        help="SQLite queue file to use for the harness.",
    )
    parser.add_argument("--messages", type=int, default=100)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=4)
    parser.add_argument("--lease-timeout", type=float, default=0.2)
    return parser.parse_args()


def _run_throughput(
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
    _clear_store(store_path)
    ctx = mp.get_context("spawn")
    start = ctx.Event()
    stop = ctx.Event()
    consumed_count = ctx.Value("i", 0)
    consumed_lock = ctx.Lock()
    report_queue = ctx.Queue()
    per_producer, remainder = divmod(messages, producers)

    processes = [
        ctx.Process(
            target=_producer_process,
            args=(
                queue_name,
                store_path,
                lease_timeout,
                start,
                report_queue,
                index,
                per_producer + (1 if index < remainder else 0),
            ),
        )
        for index in range(producers)
    ] + [
        ctx.Process(
            target=_consumer_process,
            args=(
                queue_name,
                store_path,
                lease_timeout,
                start,
                stop,
                consumed_count,
                consumed_lock,
                report_queue,
                messages,
                False,
            ),
            kwargs={"consumer_label": f"consumer-{index}"},
        )
        for index in range(consumers)
    ]

    started_at = time.perf_counter()
    _start_processes(processes)
    start.set()

    producer_processes = processes[:producers]
    consumer_processes = processes[producers:]

    for process in producer_processes:
        process.join(timeout=10.0)
    stop.set()
    for process in consumer_processes:
        process.join(timeout=10.0)

    elapsed = time.perf_counter() - started_at
    reports = _drain_reports(report_queue)
    summary = _summarize_process_reports(reports, messages, elapsed)
    summary["errors"] = _count_unexpected_exit_codes(
        processes, expected_nonzero_exitcodes=()
    )
    summary.update(
        {
            "mode": "throughput",
            "queue": queue_name,
            "store_path": str(Path(store_path).resolve()),
            "messages": messages,
            "producers": producers,
            "consumers": consumers,
            "consumed": consumed_count.value,
            "remaining": PersistentQueue(
                queue_name, store_path=store_path, lease_timeout=lease_timeout
            )
            .stats()
            .as_dict(),
        }
    )
    summary["invariants_ok"] = (
        summary["errors"] == 0
        and summary["consumed"] == messages
        and summary["unique_messages"] == messages
        and summary["remaining"]["total"] == 0
    )
    summary["process_exit_codes"] = {
        process.name: process.exitcode for process in processes
    }
    return summary


def _run_crash_recovery(
    *,
    queue_name: str,
    store_path: str,
    messages: int,
    lease_timeout: float,
) -> dict[str, Any]:
    if messages <= 0:
        raise ValueError("messages must be greater than zero")
    _clear_store(store_path)
    ctx = mp.get_context("spawn")
    start = ctx.Event()
    stop = ctx.Event()
    leased = ctx.Event()
    consumed_count = ctx.Value("i", 0)
    consumed_lock = ctx.Lock()
    report_queue = ctx.Queue()

    producer = ctx.Process(
        target=_producer_process,
        args=(queue_name, store_path, lease_timeout, start, report_queue, 0, messages),
    )
    crash_consumer = ctx.Process(
        target=_consumer_process,
        args=(
            queue_name,
            store_path,
            lease_timeout,
            start,
            stop,
            consumed_count,
            consumed_lock,
            report_queue,
            messages,
            True,
        ),
        kwargs={"leased_event": leased, "consumer_label": "crash-consumer"},
    )

    _start_processes([producer, crash_consumer])
    start.set()

    started_at = time.perf_counter()
    producer.join(timeout=10.0)
    leased.wait(timeout=10.0)
    crash_consumer.join(timeout=10.0)

    time.sleep(lease_timeout + 0.05)
    stop.set()

    recovery_consumer = ctx.Process(
        target=_consumer_process,
        args=(
            queue_name,
            store_path,
            lease_timeout,
            start,
            stop,
            consumed_count,
            consumed_lock,
            report_queue,
            messages,
            False,
        ),
        kwargs={"consumer_label": "recovery-consumer"},
    )
    recovery_consumer.start()
    recovery_consumer.join(timeout=10.0)
    elapsed = time.perf_counter() - started_at

    reports = _drain_reports(report_queue)
    summary = _summarize_process_reports(reports, messages, elapsed)
    summary["errors"] = _count_unexpected_exit_codes(
        [producer, crash_consumer, recovery_consumer], expected_nonzero_exitcodes=(3,)
    )
    remaining = (
        PersistentQueue(queue_name, store_path=store_path, lease_timeout=lease_timeout)
        .stats()
        .as_dict()
    )
    summary.update(
        {
            "mode": "crash-recovery",
            "queue": queue_name,
            "store_path": str(Path(store_path).resolve()),
            "messages": messages,
            "consumed": consumed_count.value,
            "crash_exit_code": crash_consumer.exitcode,
            "producer_exit_code": producer.exitcode,
            "recovery_exit_code": recovery_consumer.exitcode,
            "remaining": remaining,
        }
    )
    summary["invariants_ok"] = (
        summary["errors"] == 0
        and summary["consumed"] == messages
        and summary["unique_messages"] == messages
        and summary["crash_exit_code"] not in (None, 0)
        and summary["remaining"]["total"] == 0
        and summary["remaining"]["dead"] == 0
    )
    return summary


def _producer_process(
    queue_name: str,
    store_path: str,
    lease_timeout: float,
    start: Any,
    report_queue: Any,
    producer_index: int,
    message_count: int,
) -> None:
    lock_retries = Counter[str]()
    queue = PersistentQueue(
        queue_name, store_path=store_path, lease_timeout=lease_timeout
    )
    start.wait()
    for sequence in range(message_count):
        _retry_sqlite_locked(
            "put",
            lambda: queue.put({"producer": producer_index, "sequence": sequence}),
            lock_retries,
        )
    report_queue.put(
        {
            "kind": "summary",
            "role": "producer",
            "lock_retries": dict(lock_retries),
        }
    )


def _consumer_process(
    queue_name: str,
    store_path: str,
    lease_timeout: float,
    start: Any,
    stop: Any,
    consumed_count: Any,
    consumed_lock: Any,
    report_queue: Any,
    target_total: int,
    crash_after_first: bool,
    *,
    leased_event: Any | None = None,
    consumer_label: str,
) -> None:
    lock_retries = Counter[str]()
    queue = PersistentQueue(
        queue_name, store_path=store_path, lease_timeout=lease_timeout
    )
    start.wait()
    leased_once = False
    while True:
        with consumed_lock:
            if stop.is_set() and consumed_count.value >= target_total:
                break
        try:
            message = _retry_sqlite_locked(
                "get",
                lambda: queue.get_message(block=False, leased_by=consumer_label),
                lock_retries,
            )
        except Empty:
            if stop.is_set():
                with consumed_lock:
                    if consumed_count.value >= target_total:
                        break
            time.sleep(0.001)
            continue
        if crash_after_first and not leased_once:
            if leased_event is not None:
                leased_event.set()
            os._exit(3)
        leased_once = True
        _retry_sqlite_locked("ack", lambda: queue.ack(message), lock_retries)
        with consumed_lock:
            consumed_count.value += 1
        report_queue.put(
            {
                "kind": "consumed",
                "message_id": message.id,
                "consumer": consumer_label,
            }
        )
    report_queue.put(
        {
            "kind": "summary",
            "role": "consumer",
            "consumer": consumer_label,
            "lock_retries": dict(lock_retries),
        }
    )


def _retry_sqlite_locked(action: str, fn: Any, lock_retries: Counter[str]) -> Any:
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


def _summarize_process_reports(
    reports: list[dict[str, Any]], messages: int, elapsed_seconds: float
) -> dict[str, Any]:
    consumed_ids = [
        report["message_id"] for report in reports if report["kind"] == "consumed"
    ]
    lock_retries = Counter[str]()
    for report in reports:
        if report.get("kind") == "summary":
            for action, count in report.get("lock_retries", {}).items():
                lock_retries[action] += int(count)
    consumed_total = len(consumed_ids)
    throughput = consumed_total / elapsed_seconds if elapsed_seconds > 0 else 0.0
    return {
        "errors": 0,
        "error_messages": [],
        "lock_retries": dict(sorted(lock_retries.items())),
        "elapsed_seconds": round(elapsed_seconds, 6),
        "throughput_messages_per_second": round(throughput, 2),
        "consumed": consumed_total,
        "unique_messages": len(set(consumed_ids)),
        "messages": messages,
    }


def _count_unexpected_exit_codes(
    processes: Sequence[Any], *, expected_nonzero_exitcodes: tuple[int, ...]
) -> int:
    errors = 0
    for process in processes:
        exitcode = process.exitcode
        if exitcode is None:
            errors += 1
            continue
        if exitcode == 0:
            continue
        if exitcode not in expected_nonzero_exitcodes:
            errors += 1
    return errors


def _drain_reports(report_queue: Any) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    while True:
        try:
            reports.append(report_queue.get_nowait())
        except Empty:
            return reports


def _start_processes(processes: Sequence[Any]) -> None:
    for process in processes:
        process.start()


def _clear_store(store_path: str) -> None:
    store_file = Path(store_path)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{store_file}{suffix}")
        if candidate.exists():
            candidate.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
