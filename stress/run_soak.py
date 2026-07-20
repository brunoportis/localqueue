"""Executa um stress test multiprocesso reproduzível para o simpleq.

Exemplo curto:

    python stress/run_soak.py --messages 1000 --duration 30

O teste usa somente a API pública da fila. Ele pode ser executado localmente
por horas aumentando ``--duration`` e ``--messages``.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import random
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Any

from simpleq import Empty, SimpleQueue


def produce(path: str, queue_name: str, first: int, count: int) -> None:
    queue = SimpleQueue(path, name=queue_name, lease_seconds=1.0, max_retries=3)
    try:
        for index in range(first, first + count):
            job_id = f"stress-{index}"
            queue.put({"id": job_id, "producer": os.getpid()}, job_id=job_id)
    finally:
        queue.close()


def consume(
    path: str,
    queue_name: str,
    stop: Any,
    seed: int,
    nack_rate: float,
    fail_rate: float,
    crash_rate: float,
) -> None:
    queue = SimpleQueue(path, name=queue_name, lease_seconds=1.0, max_retries=3)
    rng = random.Random(seed)
    try:
        while not stop.is_set():
            try:
                job = queue.get(block=False)
            except Empty:
                time.sleep(0.005)
                continue

            choice = rng.random()
            if choice < crash_rate:
                # Simula um processo que morre depois do get e antes do ACK.
                os._exit(17)
            if choice < crash_rate + nack_rate:
                queue.nack(job, delay=0.01, last_error="stress transient error")
            elif choice < crash_rate + nack_rate + fail_rate:
                queue.fail(job, last_error="stress permanent error")
            else:
                queue.ack(job)
    finally:
        queue.close()


def database_state(path: Path, queue_name: str) -> dict[str, Any]:
    database = path / "simpleq.db"
    with sqlite3.connect(database) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        rows = connection.execute(
            """
            SELECT status, COUNT(*)
            FROM messages
            WHERE queue = ?
            GROUP BY status
            ORDER BY status
            """,
            (queue_name,),
        ).fetchall()
    counts = {str(status): count for status, count in rows}
    return {"integrity": integrity, "counts": counts, "rows": sum(counts.values())}


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.messages < 1:
        raise ValueError("--messages deve ser positivo")
    if args.duration <= 0:
        raise ValueError("--duration deve ser positivo")
    if args.consumers < 1 or args.producers < 1:
        raise ValueError("--producers e --consumers devem ser positivos")
    if args.nack_rate + args.fail_rate + args.crash_rate > 1:
        raise ValueError("as taxas de erro não podem somar mais que 1")

    context = mp.get_context("spawn")
    queue_name = "stress"
    temporary_directory = None
    if args.path is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="simpleq-stress-")
        path = Path(temporary_directory.name)
    else:
        path = Path(args.path)
        path.mkdir(parents=True, exist_ok=True)

    stop = context.Event()
    producers: list[mp.Process] = []
    consumers: list[mp.Process] = []
    messages_per_producer, remainder = divmod(args.messages, args.producers)
    first = 0

    try:
        for producer_id in range(args.producers):
            count = messages_per_producer + (producer_id < remainder)
            process = context.Process(
                target=produce,
                args=(str(path), queue_name, first, count),
                name=f"simpleq-producer-{producer_id}",
            )
            process.start()
            producers.append(process)
            first += count

        def start_consumer(consumer_id: int, restart: int = 0) -> mp.Process:
            process = context.Process(
                target=consume,
                args=(
                    str(path),
                    queue_name,
                    stop,
                    args.seed + consumer_id + restart * 100_000,
                    args.nack_rate,
                    args.fail_rate,
                    args.crash_rate,
                ),
                name=f"simpleq-consumer-{consumer_id}-{restart}",
            )
            process.start()
            return process

        for consumer_id in range(args.consumers):
            consumers.append(start_consumer(consumer_id))

        queue = SimpleQueue(str(path), name=queue_name, lease_seconds=1.0, max_retries=3)
        started_at = time.monotonic()
        restarts = [0] * args.consumers
        drained = False
        last_state: dict[str, Any] = {}

        while time.monotonic() - started_at < args.duration:
            for index, process in enumerate(consumers):
                if process.is_alive() or stop.is_set():
                    continue
                process.join()
                restarts[index] += 1
                if restarts[index] > args.max_restarts:
                    raise RuntimeError(
                        f"consumidor {index} excedeu --max-restarts"
                    )
                consumers[index] = start_consumer(index, restarts[index])

            last_state = queue.stats()
            terminal = last_state["acked"] + last_state["failed"]
            if (
                all(not process.is_alive() for process in producers)
                and last_state["ready"] == 0
                and last_state["processing"] == 0
                and terminal == args.messages
            ):
                drained = True
                break
            time.sleep(0.1)

        stop.set()
        for process in producers + consumers:
            process.join(timeout=5)
        queue.close()

        state = database_state(path, queue_name)
        producer_exitcodes = [process.exitcode for process in producers]
        consumer_exitcodes = [process.exitcode for process in consumers]
        success = (
            drained
            and state["integrity"] == "ok"
            and state["rows"] == args.messages
            and all(code == 0 for code in producer_exitcodes)
        )
        return {
            "success": success,
            "duration_seconds": round(time.monotonic() - started_at, 3),
            "messages": args.messages,
            "producers": args.producers,
            "consumers": args.consumers,
            "restarts": restarts,
            "producer_exitcodes": producer_exitcodes,
            "consumer_exitcodes": consumer_exitcodes,
            "last_stats": last_state,
            "database": state,
            "path": str(path),
        }
    finally:
        stop.set()
        for process in producers + consumers:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2)
        if temporary_directory is not None:
            temporary_directory.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=10_000)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument("--nack-rate", type=float, default=0.05)
    parser.add_argument("--fail-rate", type=float, default=0.01)
    parser.add_argument("--crash-rate", type=float, default=0.01)
    parser.add_argument("--max-restarts", type=int, default=100)
    parser.add_argument("--path", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    result = run(parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    raise SystemExit(0 if result["success"] else 1)
