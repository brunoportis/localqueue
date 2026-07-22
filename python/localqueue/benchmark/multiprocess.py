"""Spawn-only, deterministic multiprocess benchmark scenarios."""
from __future__ import annotations

import hashlib, multiprocessing, os, platform, resource, time
from pathlib import Path
from typing import Any
from localqueue import Empty, SimpleQueue
from localqueue.benchmark.metrics import MetricSummary

def _rss() -> int | None:
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value * (1024 if platform.system() == "Linux" else 1))
    except (AttributeError, OSError):
        return None

def _payload(identifier: int, requested: int) -> dict[str, Any]:
    base = {"id": f"{identifier:012d}", "producer_index": identifier, "padding": ""}
    raw = __import__("json").dumps(base, separators=(",", ":"), sort_keys=True).encode()
    base["padding"] = (hashlib.sha256(str(identifier).encode()).hexdigest() * ((requested // 64) + 2))[: max(0, requested - len(raw))]
    return base

def _producer(path: str, name: str, index: int, start: int, count: int, payload: int, fsync: bool, out: Any) -> None:
    produced = 0; samples: list[int] = []; queue = SimpleQueue(path, name, fsync=fsync)
    try:
        for identifier in range(start, start + count):
            t = time.monotonic_ns(); queue.put(_payload(identifier, payload), job_id=f"{identifier:012d}"); produced += 1
            if len(samples) < 1000: samples.append(time.monotonic_ns() - t)
        out.put({"id": f"producer-{index}", "role": "producer", "status": "passed", "produced": produced, "samples": samples, "peak_rss_bytes": _rss()})
    except Exception as exc:
        out.put({"id": f"producer-{index}", "role": "producer", "status": "failed", "produced": produced, "samples": samples, "error": str(exc), "peak_rss_bytes": _rss()})
    finally: queue.close()

def _consumer(path: str, name: str, index: int, expected: int, fsync: bool, out: Any) -> None:
    claimed = 0; acked = 0; claims: list[int] = []; roundtrips: list[int] = []; queue = SimpleQueue(path, name, fsync=fsync)
    try:
        while acked < expected:
            before = time.monotonic_ns()
            try: job = queue.get_nowait()
            except Empty: time.sleep(0.002); continue
            claims.append(time.monotonic_ns() - before); claimed += 1
            queue.ack(job); acked += 1; roundtrips.append(time.monotonic_ns() - before)
        out.put({"id": f"consumer-{index}", "role": "consumer", "status": "passed", "claimed": claimed, "acked": acked, "claim_samples": claims[:1000], "roundtrip_samples": roundtrips[:1000], "peak_rss_bytes": _rss()})
    except Exception as exc:
        out.put({"id": f"consumer-{index}", "role": "consumer", "status": "failed", "claimed": claimed, "acked": acked, "claim_samples": claims[:1000], "roundtrip_samples": roundtrips[:1000], "error": str(exc), "peak_rss_bytes": _rss()})
    finally: queue.close()

def run_multiprocess_scenario(path: Path, *, producers: int, consumers: int, messages: int, payload_bytes: int, durability: str, timeout: float = 120.0) -> dict[str, Any]:
    ctx = multiprocessing.get_context("spawn"); name = "benchmark"; results = ctx.Queue(); per = messages // producers
    ps = [ctx.Process(target=_producer, args=(str(path), name, i, i * per, per + (messages % producers if i == producers - 1 else 0), payload_bytes, durability == "full", results)) for i in range(producers)]
    cs = [ctx.Process(target=_consumer, args=(str(path), name, i, messages // consumers + (1 if i < messages % consumers else 0), durability == "full", results)) for i in range(consumers)]
    started = time.monotonic(); [p.start() for p in ps]; [p.start() for p in cs]
    for p in ps + cs: p.join(timeout=max(0.1, timeout - (time.monotonic() - started)))
    for p in ps + cs:
        if p.is_alive(): p.terminate(); p.join()
    got = []
    while not results.empty(): got.append(results.get())
    produced = sum(x.get("produced", 0) for x in got); acked = sum(x.get("acked", 0) for x in got)
    claim = [v for x in got for v in x.get("claim_samples", [])]; rt = [v for x in got for v in x.get("roundtrip_samples", [])]
    elapsed = max(1, int((time.monotonic() - started) * 1e9)); ok = produced == messages and acked == messages and all(p.exitcode == 0 for p in ps + cs)
    def series(values: list[int]) -> dict[str, Any]:
        return {"population_count": messages, "sample_count": len(values), "sampling_method": "systematic_prefix", "stride": max(1, messages // 1000), "unit": "ns", "samples": values, "summary": MetricSummary.from_samples(values or [0], elapsed, messages=messages).to_dict()}
    return {"scenario_id": f"mp-p{producers}-c{consumers}-payload{payload_bytes}-{durability}", "operation": "multiprocess_roundtrip", "parameters": {"producers": producers, "consumers": consumers, "messages": messages, "payload_requested_bytes": payload_bytes, "durability": durability}, "processes": got, "metric_series": {"claim_latency": series(claim), "roundtrip_latency": series(rt)}, "throughput": {"messages_produced": produced, "messages_acked": acked, "elapsed_ns": elapsed, "acked_per_second": acked / (elapsed / 1e9)}, "correctness": {"ok": ok, "produced": produced, "acked": acked}, "status": "passed" if ok else "failed"}
