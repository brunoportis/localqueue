"""Benchmark reproduzível dos caminhos comuns de localqueue e persist-queue.

Exemplos:

    python benchmarks/queue_bench.py --backend both --operation roundtrip
    python benchmarks/queue_bench.py --messages 10000 --output result.json

O payload usa pickle nos dois backends para que o custo de serialização não
seja confundido com a comparação do armazenamento da fila.
"""

from __future__ import annotations

import argparse
import json
import pickle
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from localqueue import SimpleQueue


class PickleSerializer:
    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, value: bytes) -> Any:
        return pickle.loads(value)


def percentile(values: list[int], quantile: float) -> int:
    ordered = sorted(values)
    index = round((len(ordered) - 1) * quantile)
    return ordered[index]


def git_revision() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def payloads(count: int, payload_bytes: int) -> list[dict[str, Any]]:
    if payload_bytes < 1:
        raise ValueError("--payload-bytes deve ser positivo")
    padding = "x" * max(0, payload_bytes - 32)
    return [{"id": index, "padding": padding} for index in range(count)]


def make_queue(backend: str, root: Path, fsync: bool) -> Any:
    if backend == "localqueue":
        return SimpleQueue(
            str(root / "localqueue"),
            lease_seconds=60.0,
            fsync=fsync,
            serializer=PickleSerializer(),
        )
    if backend == "persist-queue":
        try:
            import persistqueue
        except ImportError as error:
            raise RuntimeError(
                "instale o benchmark com `pip install -e '.[benchmark]'`"
            ) from error
        return persistqueue.SQLiteAckQueue(str(root / "persist-queue.db"))
    raise ValueError(f"backend desconhecido: {backend}")


def sqlite_settings(backend: str, queue: Any) -> dict[str, Any]:
    if backend == "localqueue":
        journal_mode, synchronous = queue._native.pragma_settings()
    else:
        connection = queue._conn
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
    synchronous_names = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}
    return {
        "journal_mode": str(journal_mode).upper(),
        "synchronous": int(synchronous),
        "synchronous_name": synchronous_names.get(int(synchronous), "UNKNOWN"),
    }


def close_queue(queue: Any) -> None:
    close = getattr(queue, "close", None)
    if close is not None:
        close()


def run_operation(
    backend: str,
    operation: str,
    messages: int,
    repetitions: int,
    payload_bytes: int,
    durability: str,
) -> dict[str, Any]:
    operation_latencies: list[int] = []
    total_elapsed_ns = 0
    settings: dict[str, Any] | None = None
    fsync = durability == "full"

    for _ in range(repetitions):
        with tempfile.TemporaryDirectory(prefix="localqueue-bench-") as directory:
            root = Path(directory)
            queue = make_queue(backend, root, fsync)
            items = payloads(messages, payload_bytes)
            try:
                if settings is None:
                    settings = sqlite_settings(backend, queue)
                if operation == "read_ack":
                    for item in items:
                        queue.put(item)

                started = time.perf_counter_ns()
                if operation == "write":
                    for item in items:
                        item_started = time.perf_counter_ns()
                        queue.put(item)
                        operation_latencies.append(time.perf_counter_ns() - item_started)
                elif operation == "read_ack":
                    for _ in items:
                        item_started = time.perf_counter_ns()
                        item = queue.get(block=False)
                        queue.ack(item)
                        operation_latencies.append(time.perf_counter_ns() - item_started)
                elif operation == "roundtrip":
                    for item in items:
                        item_started = time.perf_counter_ns()
                        queue.put(item)
                        received = queue.get(block=False)
                        queue.ack(received)
                        operation_latencies.append(time.perf_counter_ns() - item_started)
                else:
                    raise ValueError(f"operação desconhecida: {operation}")
                total_elapsed_ns += time.perf_counter_ns() - started
            finally:
                close_queue(queue)

    elapsed_seconds = total_elapsed_ns / 1_000_000_000
    return {
        "backend": backend,
        "operation": operation,
        "messages": messages,
        "repetitions": repetitions,
        "payload_bytes": payload_bytes,
        "durability": durability,
        "sqlite": settings,
        "throughput_messages_per_second": messages * repetitions / elapsed_seconds,
        "latency_microseconds": {
            "p50": percentile(operation_latencies, 0.50) / 1_000,
            "p95": percentile(operation_latencies, 0.95) / 1_000,
            "p99": percentile(operation_latencies, 0.99) / 1_000,
            "mean": statistics.fmean(operation_latencies) / 1_000,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        choices=("localqueue", "persist-queue", "both"),
        default="both",
    )
    parser.add_argument(
        "--operation",
        choices=("write", "read_ack", "roundtrip"),
        default="roundtrip",
    )
    parser.add_argument("--messages", type=int, default=1_000)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument(
        "--durability",
        choices=("normal", "full"),
        default="normal",
        help="política do localqueue; persist-queue mantém sua configuração padrão",
    )
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.messages < 1 or args.repetitions < 1:
        raise ValueError("--messages e --repetitions devem ser positivos")

    backends = ("localqueue", "persist-queue") if args.backend == "both" else (args.backend,)
    result = {
        "revision": git_revision(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "results": [
            run_operation(
                backend,
                args.operation,
                args.messages,
                args.repetitions,
                args.payload_bytes,
                args.durability,
            )
            for backend in backends
        ],
    }
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is None:
        print(encoded)
    else:
        args.output.write_text(encoded + "\n", encoding="utf-8")
        print(f"benchmark salvo em {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
