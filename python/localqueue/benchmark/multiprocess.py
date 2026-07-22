"""Spawn-only multiprocess benchmark implementation."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import platform
import time
from pathlib import Path
from typing import Any

from localqueue import Empty, SimpleQueue
from localqueue.benchmark.metrics import MetricSummary

try:  # resource is unavailable on Windows
    import resource as _resource
except ImportError:  # pragma: no cover - exercised on Windows
    _resource = None


def peak_rss_bytes() -> int | None:
    if _resource is None or platform.system() not in {"Linux", "Darwin"}:
        return None
    value = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024 if platform.system() == "Linux" else value)


def make_payload(
    identifier: int, producer_index: int, requested: int
) -> tuple[dict[str, Any], int]:
    value: dict[str, Any] = {
        "id": f"{identifier:012d}",
        "producer_index": producer_index,
        "created_ns": time.monotonic_ns(),
        "padding": "",
    }
    envelope = len(json.dumps(value, separators=(",", ":"), sort_keys=True).encode())
    value["padding"] = (
        hashlib.sha256(f"{identifier}:{producer_index}".encode()).hexdigest()
        * (requested // 64 + 2)
    )[: max(0, requested - envelope)]
    return value, len(json.dumps(value, separators=(",", ":"), sort_keys=True).encode())


def producer_target(
    path: str,
    name: str,
    index: int,
    start: int,
    count: int,
    requested: int,
    full: bool,
    ready: Any,
    done: Any,
    output: Any,
) -> None:
    produced = 0
    puts: list[int] = []
    actual = 0
    queue = SimpleQueue(path, name, fsync=full)
    try:
        ready.wait()
        for identifier in range(start, start + count):
            value, actual = make_payload(identifier, index, requested)
            before = time.monotonic_ns()
            queue.put(value, job_id=value["id"])
            puts.append(time.monotonic_ns() - before)
            produced += 1
        output.put(
            {
                "id": f"producer-{index}",
                "role": "producer",
                "status": "passed",
                "exit_code": 0,
                "produced": produced,
                "claim_samples": [],
                "roundtrip_samples": [],
                "put_samples": puts,
                "actual_serialized_bytes": actual,
                "peak_rss_bytes": peak_rss_bytes(),
            }
        )
    except Exception as exc:
        output.put(
            {
                "id": f"producer-{index}",
                "role": "producer",
                "status": "failed",
                "exit_code": 1,
                "produced": produced,
                "error": str(exc),
                "peak_rss_bytes": peak_rss_bytes(),
            }
        )
        raise
    finally:
        queue.close()


def consumer_target(
    path: str,
    name: str,
    index: int,
    total: int,
    producers_done: Any,
    ready: Any,
    output: Any,
    full: bool,
    timeout: float,
) -> None:
    claimed = acked = 0
    claims: list[int] = []
    roundtrips: list[int] = []
    queue = SimpleQueue(path, name, fsync=full)
    deadline = time.monotonic() + timeout
    try:
        ready.wait()
        while time.monotonic() < deadline:
            before = time.monotonic_ns()
            try:
                job = queue.get(timeout=0.05)
            except Empty:
                if (
                    producers_done.is_set()
                    and queue.stats().get("ready", 0) == 0
                    and queue.stats().get("processing", 0) == 0
                ):
                    break
                continue
            claim_done = time.monotonic_ns()
            claims.append(claim_done - before)
            claimed += 1
            queue.ack(job)
            acked += 1
            created_ns = (
                job.data.get("created_ns") if isinstance(job.data, dict) else None
            )
            if not isinstance(created_ns, int):
                raise RuntimeError("payload created_ns is missing or invalid")
            roundtrips.append(time.monotonic_ns() - created_ns)
        output.put(
            {
                "id": f"consumer-{index}",
                "role": "consumer",
                "status": "passed",
                "exit_code": 0,
                "claimed": claimed,
                "acked": acked,
                "claim_samples": claims,
                "roundtrip_samples": roundtrips,
                "peak_rss_bytes": peak_rss_bytes(),
            }
        )
    except Exception as exc:
        output.put(
            {
                "id": f"consumer-{index}",
                "role": "consumer",
                "status": "failed",
                "exit_code": 1,
                "claimed": claimed,
                "acked": acked,
                "error": str(exc),
                "peak_rss_bytes": peak_rss_bytes(),
            }
        )
        raise
    finally:
        queue.close()


def _series(
    values: list[int], population: int, elapsed: int, limit: int = 1000
) -> dict[str, Any]:
    stride = max(1, (population + limit - 1) // limit)
    samples = values[::stride]
    if not samples:
        raise RuntimeError("required metric series has no samples")
    return {
        "population_count": population,
        "sample_count": len(samples),
        "limit": limit,
        "method": "systematic",
        "stride": stride,
        "unit": "ns",
        "samples": samples,
        "summary": MetricSummary.from_samples(
            samples, elapsed, messages=population
        ).to_dict(),
    }


def run_multiprocess_scenario(
    path: Path,
    *,
    producers: int,
    consumers: int,
    messages: int,
    payload_bytes: int,
    durability: str,
    timeout: float = 120.0,
) -> dict[str, Any]:
    scenario_path = (
        path / f"p{producers}c{consumers}-m{messages}-b{payload_bytes}-{durability}"
    )
    scenario_path.mkdir(parents=True, exist_ok=False)
    name = "benchmark"
    ctx = multiprocessing.get_context("spawn")
    output = ctx.Queue()
    ready = ctx.Barrier(producers + consumers + 1)
    done = ctx.Event()
    per = messages // producers
    ps = [
        ctx.Process(
            target=producer_target,
            args=(
                str(scenario_path),
                name,
                i,
                i * per,
                per + (messages % producers if i == producers - 1 else 0),
                payload_bytes,
                durability == "full",
                ready,
                done,
                output,
            ),
        )
        for i in range(producers)
    ]
    cs = [
        ctx.Process(
            target=consumer_target,
            args=(
                str(scenario_path),
                name,
                i,
                messages,
                done,
                ready,
                output,
                durability == "full",
                timeout,
            ),
        )
        for i in range(consumers)
    ]
    started = time.monotonic()
    [p.start() for p in ps + cs]
    ready.wait()
    producer_results = []
    for process in ps:
        process.join(timeout)
    for process in ps:
        if process.is_alive():
            process.terminate()
            process.join(5)
    # The parent, and only the parent, publishes the global completion signal.
    producer_results = []
    while len(producer_results) < producers:
        try:
            result = output.get(timeout=1.0)
        except Exception:
            break
        producer_results.append(result)
    valid_producers = {r.get("id") for r in producer_results}
    if (
        len(valid_producers) == producers
        and all(
            r.get("status") == "passed" and r.get("exit_code") == 0
            for r in producer_results
        )
        and all(p.exitcode == 0 for p in ps)
    ):
        done.set()
    for process in cs:
        process.join(timeout)
    for process in ps + cs:
        if process.is_alive():
            process.terminate()
            process.join(5)
    results = producer_results
    while len(results) < producers + consumers:
        try:
            results.append(output.get(timeout=1.0))
        except Exception:
            break
    output.close()
    output.join_thread()
    elapsed = max(1, int((time.monotonic() - started) * 1e9))
    produced = sum(r.get("produced", 0) for r in results)
    claimed = sum(r.get("claimed", 0) for r in results)
    acked = sum(r.get("acked", 0) for r in results)
    queue = SimpleQueue(scenario_path, name, fsync=durability == "full")
    stats = queue.stats()
    integrity = queue.check_integrity(mode="full").to_dict()
    queue.close()
    claims = sorted((v for r in results for v in r.get("claim_samples", [])))
    roundtrips = sorted((v for r in results for v in r.get("roundtrip_samples", [])))
    ok = (
        produced == claimed == acked == messages
        and stats.get("ready") == 0
        and stats.get("processing") == 0
        and stats.get("failed", 0) == 0
        and integrity.get("ok", False)
        and all(p.exitcode == 0 for p in ps + cs)
    )
    return {
        "scenario_id": f"mp-p{producers}-c{consumers}-payload{payload_bytes}-{durability}",
        "operation": "multiprocess_roundtrip",
        "parameters": {
            "producers": producers,
            "consumers": consumers,
            "messages": messages,
            "payload_requested_bytes": payload_bytes,
            "durability": durability,
        },
        "processes": sorted(results, key=lambda r: r["id"]),
        "metric_series": {
            "claim_latency": _series(claims, claimed, elapsed),
            "roundtrip_latency": _series(roundtrips, acked, elapsed),
        },
        "throughput": {
            "messages_produced": produced,
            "messages_claimed": claimed,
            "messages_acked": acked,
            "elapsed_ns": elapsed,
            "acked_per_second": acked / (elapsed / 1e9),
        },
        "correctness": {"ok": ok, "stats": stats, "integrity": integrity},
        "status": "passed" if ok else "failed",
        "files": {},
    }
