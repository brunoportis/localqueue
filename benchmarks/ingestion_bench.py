"""Representative benchmark for EventBus bulk ingestion.

Each scenario runs in a fresh spawned process so peak RSS is comparable.
The report covers durable identity, capacity checks, fan-out, batch sizes,
repeated ``dispatch()``, native transaction duration, and Python allocations.
It is operational evidence, not a release performance threshold.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import multiprocessing
import statistics
import sys
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path

from localqueue.bus import BaseEvent, BusTopology, EventBus, event

try:
    import resource
except ImportError:  # pragma: no cover - exercised by the Windows CI matrix
    resource = None


class PlainContact(BaseEvent):
    key: str


@event(identity="key")
class IdentifiedContact(BaseEvent):
    key: str


@dataclass(frozen=True)
class Scenario:
    operation: str
    identity: bool
    fanout: int
    batch_size: int
    capacity: bool
    rows: int


class _TimedNative:
    def __init__(self, native: object) -> None:
        self._native = native
        self.transactions: list[float] = []

    def _record(self, method: str, *args: object) -> object:
        started = time.perf_counter()
        try:
            return getattr(self._native, method)(*args)
        finally:
            self.transactions.append(time.perf_counter() - started)

    def _enqueue_batch_with_identity(self, entries: object, capacity: object) -> object:
        return self._record("_enqueue_batch_with_identity", entries, capacity)

    def _fanout_with_identity(self, payload: object, targets: object) -> object:
        return self._record("_fanout_with_identity", payload, targets)

    def close(self) -> object:
        return getattr(self._native, "close")()


def scenario_matrix(rows: int) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for identity in (False, True):
        for fanout in (1, 5):
            for batch_size in (100, 1_000, 10_000):
                for capacity in (False, True):
                    scenarios.append(
                        Scenario(
                            operation="ingest",
                            identity=identity,
                            fanout=fanout,
                            batch_size=batch_size,
                            capacity=capacity,
                            rows=rows,
                        )
                    )
            scenarios.append(
                Scenario(
                    operation="dispatch",
                    identity=identity,
                    fanout=fanout,
                    batch_size=1,
                    capacity=False,
                    rows=rows,
                )
            )
    return scenarios


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _process_peak_rss_bytes() -> int:
    """Return peak process RSS using the platform's standard facilities."""
    if resource is not None:
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return peak_rss if sys.platform == "darwin" else peak_rss * 1024
    if sys.platform != "win32":
        raise RuntimeError("peak RSS is unavailable on this platform")

    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("page_fault_count", wintypes.DWORD),
            ("peak_working_set_size", ctypes.c_size_t),
            ("working_set_size", ctypes.c_size_t),
            ("quota_peak_paged_pool_usage", ctypes.c_size_t),
            ("quota_paged_pool_usage", ctypes.c_size_t),
            ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
            ("quota_non_paged_pool_usage", ctypes.c_size_t),
            ("pagefile_usage", ctypes.c_size_t),
            ("peak_pagefile_usage", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.argtypes = []
    get_current_process.restype = wintypes.HANDLE

    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    get_process_memory_info = psapi.GetProcessMemoryInfo
    get_process_memory_info.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    get_process_memory_info.restype = wintypes.BOOL

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    success = get_process_memory_info(
        get_current_process(), ctypes.byref(counters), counters.cb
    )
    if not success:
        raise ctypes.WinError(ctypes.get_last_error())
    return int(counters.peak_working_set_size)


def _run_scenario(scenario: Scenario) -> dict[str, object]:
    event_type = IdentifiedContact if scenario.identity else PlainContact
    topology = BusTopology(
        {f"contacts-{index}": ["*"] for index in range(scenario.fanout)}
    )
    with tempfile.TemporaryDirectory(prefix="localqueue-ingestion-bench-") as root:
        bus = EventBus(str(Path(root) / "bus"), name="benchmark", topology=topology)
        timed_native = _TimedNative(bus._native_queue)
        bus._native_queue = timed_native
        tracemalloc.start()
        started = time.perf_counter()
        try:
            if scenario.operation == "ingest":
                result = asyncio.run(
                    bus.ingest(
                        (event_type(key=str(index)) for index in range(scenario.rows)),
                        batch_size=scenario.batch_size,
                        max_pending=scenario.rows if scenario.capacity else None,
                    )
                )
                deliveries = result.deliveries_total
            else:
                for index in range(scenario.rows):
                    bus.dispatch(event_type(key=str(index)))
                deliveries = scenario.rows * scenario.fanout
            elapsed = time.perf_counter() - started
            _current, python_peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
            bus.close()
    durations = timed_native.transactions
    return {
        **asdict(scenario),
        "elapsed_seconds": elapsed,
        "items_per_second": scenario.rows / elapsed,
        "deliveries_per_second": deliveries / elapsed,
        "native_transactions": len(durations),
        "transaction_mean_seconds": statistics.fmean(durations),
        "transaction_p95_seconds": _percentile(durations, 0.95),
        "transaction_max_seconds": max(durations),
        "python_peak_allocated_bytes": python_peak,
        "process_peak_rss_bytes": _process_peak_rss_bytes(),
    }


def _child(scenario: Scenario, connection: object) -> None:
    try:
        connection.send(("ok", _run_scenario(scenario)))
    except BaseException as error:
        connection.send(("error", f"{type(error).__name__}: {error}"))
    finally:
        connection.close()


def run_matrix(rows: int) -> list[dict[str, object]]:
    context = multiprocessing.get_context("spawn")
    results: list[dict[str, object]] = []
    for scenario in scenario_matrix(rows):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=_child, args=(scenario, sender))
        process.start()
        sender.close()
        status, payload = receiver.recv()
        process.join()
        if status != "ok" or process.exitcode != 0:
            raise RuntimeError(f"benchmark scenario failed: {payload}")
        results.append(payload)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.rows < 10_000:
        parser.error("--rows must be at least 10000 to exercise every batch size")
    report = {
        "schema_version": 1,
        "notes": [
            "capacity=true sets max_pending=rows, exercising capacity checks "
            "without intentional backpressure",
            "process_peak_rss_bytes includes interpreter and extension baseline",
            "python_peak_allocated_bytes excludes Rust and SQLite allocations",
        ],
        "scenarios": run_matrix(args.rows),
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
