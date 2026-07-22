"""Spawn-only multiprocess benchmark implementation."""

from __future__ import annotations

import functools
import hashlib
import multiprocessing
import operator
import platform
import queue as queue_module
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from localqueue import Empty, JsonSerializer, LocalQueueError, SimpleQueue
from localqueue.benchmark.environment import environment, subject
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.metrics import MetricSummary
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult
from localqueue.benchmark.multiprocess_models import IDValidation, MultiprocessConfig
from localqueue.benchmark.profiles import multiprocess_matrix
from localqueue.benchmark.runner import _atomic_write

try:  # resource is unavailable on Windows
    import resource as _resource
except ImportError:  # pragma: no cover - exercised on Windows
    _resource = None


def peak_rss_bytes() -> int | None:
    if _resource is None or platform.system() not in {"Linux", "Darwin"}:
        return None
    value = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
    return int(value * 1024 if platform.system() == "Linux" else value)


def rss_method() -> str | None:
    if _resource is None or platform.system() not in {"Linux", "Darwin"}:
        return None
    return "resource.getrusage(RUSAGE_SELF).ru_maxrss"


def make_payload(
    identifier: int, producer_index: int, requested: int
) -> tuple[dict[str, Any], int]:
    value: dict[str, Any] = {
        "id": f"{identifier:012d}",
        "producer_index": producer_index,
        "created_ns": time.monotonic_ns(),
        "padding": "",
    }
    serializer = JsonSerializer()
    envelope = len(serializer.dumps(value))
    value["padding"] = (
        hashlib.sha256(f"{identifier}:{producer_index}".encode()).hexdigest()
        * (requested // 64 + 2)
    )[: max(0, requested - envelope)]
    return value, len(serializer.dumps(value))


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
    sample_stride: int,
) -> None:
    produced = 0
    puts: list[tuple[int, int]] = []
    actual = 0
    queue = SimpleQueue(path, name, fsync=full)
    try:
        ready.wait()
        for identifier in range(start, start + count):
            value, actual = make_payload(identifier, index, requested)
            while True:
                before = time.monotonic_ns()
                value["created_ns"] = before
                actual = len(JsonSerializer().dumps(value))
                try:
                    queue.put(value, job_id=value["id"])
                    break
                except LocalQueueError as exc:
                    if "database is locked" not in str(exc).lower():
                        raise
                    time.sleep(0.002)
            if identifier % sample_stride == 0:
                puts.append((identifier, time.monotonic_ns() - before))
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
                "rss_method": rss_method(),
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
                "rss_method": rss_method(),
            }
        )
        raise SystemExit(1) from None
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
    sample_stride: int,
    exact_ids: bool,
) -> None:
    claimed = acked = 0
    consumed_ids: list[int] = []
    id_count = id_sum = id_xor = id_digest = out_of_range = 0
    claims: list[tuple[int, int]] = []
    roundtrips: list[tuple[int, int]] = []
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
            except LocalQueueError as exc:
                if "database is locked" not in str(exc).lower():
                    raise
                time.sleep(0.002)
                continue
            claim_done = time.monotonic_ns()
            message_id = int(job.data["id"]) if isinstance(job.data, dict) else -1
            if exact_ids:
                consumed_ids.append(message_id)
            id_count += 1
            id_sum += message_id
            id_xor ^= message_id
            id_digest = (
                id_digest
                + int.from_bytes(
                    hashlib.sha256(str(message_id).encode()).digest(), "big"
                )
            ) % (1 << 256)
            if message_id < 0 or message_id >= total:
                out_of_range += 1
            if message_id % sample_stride == 0:
                claims.append((message_id, claim_done - before))
            claimed += 1
            queue.ack(job)
            acked += 1
            created_ns = (
                job.data.get("created_ns") if isinstance(job.data, dict) else None
            )
            if not isinstance(created_ns, int):
                raise RuntimeError("payload created_ns is missing or invalid")
            if message_id % sample_stride == 0:
                roundtrips.append((message_id, time.monotonic_ns() - created_ns))
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
                "consumed_ids": consumed_ids,
                "id_aggregate": {
                    "count": id_count,
                    "sum": id_sum,
                    "xor": id_xor,
                    "digest": f"{id_digest:064x}",
                    "out_of_range": out_of_range,
                },
                "peak_rss_bytes": peak_rss_bytes(),
                "rss_method": rss_method(),
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
                "consumed_ids": consumed_ids,
                "id_aggregate": {
                    "count": id_count,
                    "sum": id_sum,
                    "xor": id_xor,
                    "digest": f"{id_digest:064x}",
                    "out_of_range": out_of_range,
                },
                "error": str(exc),
                "peak_rss_bytes": peak_rss_bytes(),
                "rss_method": rss_method(),
            }
        )
        raise SystemExit(1) from None
    finally:
        queue.close()


def _series(
    values: list[tuple[int, int]], population: int, elapsed: int, limit: int = 1000
) -> dict[str, Any]:
    stride = max(1, (population + limit - 1) // limit)
    samples = sorted(values, key=lambda sample: sample[0])[:limit]
    if not samples:
        raise RuntimeError("required metric series has no samples")
    return {
        "population_count": population,
        "sample_count": len(samples),
        "limit": limit,
        "method": "systematic",
        "ordering_key": "global_message_id",
        "stride": stride,
        "unit": "ns",
        "samples": samples,
        "summary": MetricSummary.from_samples(
            [latency for _, latency in samples], elapsed, messages=population
        ).to_dict(),
    }


def validate_ids(ids: list[int], messages: int, *, exact: bool) -> IDValidation:
    expected_ids = list(range(messages)) if exact else []
    expected_common = {
        "count": messages,
        "sum": messages * (messages - 1) // 2,
        "xor": functools.reduce(operator.xor, range(messages), 0),
        "sha256": hashlib.sha256(
            ",".join(map(str, range(messages))).encode()
        ).hexdigest(),
    }
    ordered = sorted(ids)
    observed_common = {
        "count": len(ids),
        "sum": sum(ids),
        "xor": functools.reduce(operator.xor, ids, 0),
        "sha256": hashlib.sha256(",".join(map(str, ordered)).encode()).hexdigest(),
    }
    if exact:
        expected = {**expected_common, "ids": expected_ids}
        observed = {**observed_common, "ids": ordered}
        method = "exact"
    else:
        expected = expected_common
        observed = observed_common
        method = "count_sum_xor_sha256"
    return IDValidation(
        method=method, expected=expected, observed=observed, ok=expected == observed
    )


def validate_id_aggregates(
    aggregates: list[dict[str, Any]], messages: int
) -> IDValidation:
    modulus = 1 << 256
    expected_digest = (
        sum(
            int.from_bytes(hashlib.sha256(str(identifier).encode()).digest(), "big")
            for identifier in range(messages)
        )
        % modulus
    )
    observed = {
        "count": sum(item["count"] for item in aggregates),
        "sum": sum(item["sum"] for item in aggregates),
        "xor": functools.reduce(operator.xor, (item["xor"] for item in aggregates), 0),
        "digest": f"{sum(int(item['digest'], 16) for item in aggregates) % modulus:064x}",
        "out_of_range": sum(item["out_of_range"] for item in aggregates),
    }
    expected = {
        "count": messages,
        "sum": messages * (messages - 1) // 2,
        "xor": functools.reduce(operator.xor, range(messages), 0),
        "digest": f"{expected_digest:064x}",
        "out_of_range": 0,
    }
    return IDValidation(
        method="count_sum_xor_sha256_sum",
        expected=expected,
        observed=observed,
        ok=expected == observed,
    )


def _file_snapshot(database: Path) -> dict[str, dict[str, int | bool | None]]:
    def one(path: Path) -> dict[str, int | bool | None]:
        return {
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() else None,
        }

    return {
        "database": one(database),
        "wal": one(Path(f"{database}-wal")),
        "shm": one(Path(f"{database}-shm")),
    }


def _sqlite_settings(queue: SimpleQueue) -> dict[str, Any]:
    diagnostics = queue.diagnostics()
    return {
        "journal_mode": diagnostics.journal_mode,
        "synchronous": diagnostics.synchronous,
        "synchronous_name": {1: "NORMAL", 2: "FULL"}.get(
            diagnostics.synchronous, "UNKNOWN"
        ),
        "durability_mode": diagnostics.durability_mode,
        "busy_timeout_ms": diagnostics.busy_timeout_ms,
        "page_size": diagnostics.page_size,
        "sqlite_version": diagnostics.sqlite_version,
    }


def _cleanup_children(
    processes: list[Any], output: Any, run_path: Path, keep_workdir: bool
) -> None:
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join(1)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join()
    output.close()
    output.join_thread()
    if not keep_workdir:
        shutil.rmtree(run_path, ignore_errors=True)


def run_large_database_scenario(
    path: Path,
    *,
    rows: int,
    durability: str,
    payload_bytes: int = 100,
    batch_size: int = 1000,
    keep_workdir: bool = False,
) -> dict[str, Any]:
    path.mkdir(parents=True, exist_ok=True)
    run_path = Path(tempfile.mkdtemp(prefix="localqueue-large-db-", dir=path))
    database = run_path / "localqueue.db"
    queue = SimpleQueue(str(run_path), "large-database", fsync=durability == "full")
    snapshots: dict[str, Any] = {"before_workload": _file_snapshot(database)}
    before_stats = queue.stats()
    try:
        started = time.monotonic_ns()
        actual_payload = 0
        for start in range(0, rows, batch_size):
            items: list[Any] = []
            for identifier in range(start, min(rows, start + batch_size)):
                payload, actual_payload = make_payload(identifier, 0, payload_bytes)
                items.append(payload)
            queue.put_many(items)
        preload_elapsed = time.monotonic_ns() - started
        snapshots["after_preload"] = _file_snapshot(database)
        stats_after_preload = queue.stats()
        measured = min(rows, 1000)
        measure_started = time.monotonic_ns()
        for _ in range(measured):
            job = queue.get()
            queue.ack(job)
        measure_elapsed = time.monotonic_ns() - measure_started
        snapshots["after_drain"] = _file_snapshot(database)
        stats_after = queue.stats()
        integrity = queue.check_integrity(mode="full").to_dict()
        sqlite = _sqlite_settings(queue)
    finally:
        queue.close()
        if not keep_workdir and sys.exc_info()[0] is not None:
            shutil.rmtree(run_path, ignore_errors=True)
    snapshots["after_close"] = _file_snapshot(database)
    ok = (
        stats_after["ready"] == rows - measured
        and stats_after["processing"] == 0
        and integrity["ok"]
    )
    result = {
        "scenario_id": f"mp-large-db-{rows}-{durability}",
        "operation": "multiprocess_large_database",
        "parameters": {
            "durability": durability,
            "payload_requested_bytes": payload_bytes,
            "payload_serialized_bytes": actual_payload,
            "serializer": "localqueue.JsonSerializer",
            "padding_method": "deterministic_sha256_repetition",
        },
        "large_database": {
            "target_rows": rows,
            "actual_rows": stats_after_preload["ready"],
            "batch_size": batch_size,
            "preload_elapsed_ns": preload_elapsed,
            "measured_claims": measured,
        },
        "throughput": {
            "messages_claimed": measured,
            "messages_acked": measured,
            "elapsed_ns": measure_elapsed,
            "acked_per_second": measured / (measure_elapsed / 1e9),
        },
        "sqlite": sqlite,
        "files": snapshots,
        "correctness": {
            "ok": ok,
            "stats_before": before_stats,
            "stats_after_preload": stats_after_preload,
            "stats_after": stats_after,
            "integrity": integrity,
        },
        "status": "passed" if ok else "failed",
    }
    if not keep_workdir:
        shutil.rmtree(run_path, ignore_errors=True)
    return result


def run_multiprocess_scenario(
    path: Path,
    *,
    producers: int,
    consumers: int,
    messages: int,
    payload_bytes: int,
    durability: str,
    timeout: float = 120.0,
    exact_id_validation: bool = True,
    keep_workdir: bool = False,
    sample_limit: int = 1000,
) -> dict[str, Any]:
    path.mkdir(parents=True, exist_ok=True)
    run_path = Path(tempfile.mkdtemp(prefix="localqueue-mp-run-", dir=path))
    scenario_path = (
        run_path / f"p{producers}c{consumers}-m{messages}-b{payload_bytes}-{durability}"
    )
    scenario_path.mkdir()
    name = "benchmark"
    database = scenario_path / "localqueue.db"
    file_phases: dict[str, Any] = {"before_workload": _file_snapshot(database)}
    initializer = SimpleQueue(str(scenario_path), name, fsync=durability == "full")
    sqlite = _sqlite_settings(initializer)
    initializer.close()
    ctx = multiprocessing.get_context("spawn")
    output = ctx.Queue()
    ready = ctx.Barrier(producers + consumers + 1)
    done = ctx.Event()
    per = messages // producers
    sample_stride = max(1, (messages + sample_limit - 1) // sample_limit)
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
                sample_stride,
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
                sample_stride,
                exact_id_validation,
            ),
        )
        for i in range(consumers)
    ]
    process_by_id = {
        **{f"producer-{index}": process for index, process in enumerate(ps)},
        **{f"consumer-{index}": process for index, process in enumerate(cs)},
    }
    role_by_id = {
        **{f"producer-{index}": "producer" for index in range(producers)},
        **{f"consumer-{index}": "consumer" for index in range(consumers)},
    }
    producer_ids = {f"producer-{index}" for index in range(producers)}
    started = time.monotonic()
    deadline = started + timeout
    try:
        for process in ps + cs:
            process.start()
        ready.wait(timeout=max(0.001, deadline - time.monotonic()))
    except BaseException:
        _cleanup_children(ps + cs, output, run_path, keep_workdir)
        raise
    result_by_id: dict[str, dict[str, Any]] = {}
    protocol_errors: list[str] = []

    def collect_one() -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            result = output.get(timeout=min(0.1, remaining))
        except queue_module.Empty:
            return
        if not isinstance(result, dict):
            protocol_errors.append("malformed worker result")
            return
        logical_id = result.get("id")
        if logical_id not in process_by_id:
            protocol_errors.append(f"unknown worker result: {logical_id}")
        elif logical_id in result_by_id:
            protocol_errors.append(f"duplicate worker result: {logical_id}")
        elif result.get("role") != role_by_id[logical_id]:
            protocol_errors.append(f"incorrect role for {logical_id}")
        else:
            result_by_id[logical_id] = result

    while time.monotonic() < deadline:
        collect_one()
        if producer_ids.issubset(result_by_id) and not any(p.is_alive() for p in ps):
            break
    if (
        producer_ids.issubset(result_by_id)
        and all(process.exitcode == 0 for process in ps)
        and all(
            result_by_id[logical_id].get("status") == "passed"
            and result_by_id[logical_id].get("exit_code") == 0
            for logical_id in producer_ids
        )
    ):
        file_phases["after_producers"] = _file_snapshot(database)
        done.set()
    while len(result_by_id) < len(process_by_id) and time.monotonic() < deadline:
        collect_one()
    for process in ps + cs:
        process.join(timeout=max(0.0, deadline - time.monotonic()))
    for process in ps + cs:
        if process.is_alive():
            process.terminate()
            process.join(timeout=max(0.0, deadline - time.monotonic()))
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join()
    for logical_id, process in process_by_id.items():
        result = result_by_id.get(logical_id)
        if result is None:
            protocol_errors.append(f"missing worker result: {logical_id}")
        elif result.get("exit_code") != process.exitcode:
            protocol_errors.append(f"exit code mismatch for {logical_id}")
        elif result.get("status") == "passed" and process.exitcode != 0:
            protocol_errors.append(f"non-zero successful worker: {logical_id}")
    results = list(result_by_id.values())
    output.close()
    output.join_thread()
    elapsed = max(1, int((time.monotonic() - started) * 1e9))
    produced = sum(r.get("produced", 0) for r in results)
    claimed = sum(r.get("claimed", 0) for r in results)
    acked = sum(r.get("acked", 0) for r in results)
    actual_sizes = {
        r["actual_serialized_bytes"]
        for r in results
        if r.get("role") == "producer" and r.get("actual_serialized_bytes")
    }
    actual_serialized_bytes = (
        next(iter(actual_sizes)) if len(actual_sizes) == 1 else None
    )
    queue = SimpleQueue(str(scenario_path), name, fsync=durability == "full")
    try:
        stats = queue.stats()
        integrity = queue.check_integrity(mode="full").to_dict()
        file_phases["after_drain"] = _file_snapshot(database)
    except BaseException:
        queue.close()
        if not keep_workdir:
            shutil.rmtree(run_path, ignore_errors=True)
        raise
    finally:
        queue.close()
    file_phases["after_close"] = _file_snapshot(database)
    claims = [v for r in results for v in r.get("claim_samples", [])]
    roundtrips = [v for r in results for v in r.get("roundtrip_samples", [])]
    if exact_id_validation:
        consumed_ids = [
            identifier for r in results for identifier in r.get("consumed_ids", [])
        ]
        id_validation = validate_ids(consumed_ids, messages, exact=True)
    else:
        id_validation = validate_id_aggregates(
            [r["id_aggregate"] for r in results if r.get("role") == "consumer"],
            messages,
        )
    ok = (
        produced == claimed == acked == messages
        and stats.get("ready") == 0
        and stats.get("processing") == 0
        and stats.get("failed", 0) == 0
        and integrity.get("ok", False)
        and all(p.exitcode == 0 for p in ps + cs)
        and not protocol_errors
        and not any(p.is_alive() for p in ps + cs)
        and id_validation.ok
    )
    try:
        claim_series = _series(claims, claimed, elapsed, sample_limit)
        roundtrip_series = _series(roundtrips, acked, elapsed, sample_limit)
    except BaseException:
        if not keep_workdir:
            shutil.rmtree(run_path, ignore_errors=True)
        raise
    result = {
        "scenario_id": f"mp-p{producers}-c{consumers}-payload{payload_bytes}-{durability}",
        "operation": "multiprocess_roundtrip",
        "parameters": {
            "producers": producers,
            "consumers": consumers,
            "messages": messages,
            "payload_requested_bytes": payload_bytes,
            "payload_serialized_bytes": actual_serialized_bytes,
            "serializer": "localqueue.JsonSerializer",
            "padding_method": "deterministic_sha256_repetition",
            "durability": durability,
        },
        "processes": sorted(results, key=lambda r: r["id"]),
        "metric_series": {
            "claim_latency": claim_series,
            "roundtrip_latency": roundtrip_series,
        },
        "throughput": {
            "messages_produced": produced,
            "messages_claimed": claimed,
            "messages_acked": acked,
            "elapsed_ns": elapsed,
            "produced_per_second": produced / (elapsed / 1e9),
            "acked_per_second": acked / (elapsed / 1e9),
        },
        "correctness": {
            "ok": ok,
            "stats": stats,
            "integrity": integrity,
            "worker_protocol_errors": protocol_errors,
            "id_validation": id_validation.to_dict(),
        },
        "status": "passed" if ok else "failed",
        "sqlite": sqlite,
        "files": file_phases,
    }
    if not keep_workdir:
        shutil.rmtree(run_path, ignore_errors=True)
    return result


def run_multiprocess_profile(
    config: MultiprocessConfig,
    output: Path | None = None,
    workdir: Path | None = None,
) -> BenchmarkReport:
    """Run a named multiprocess profile and atomically preserve partial reports."""
    root = workdir or Path.cwd() / "localqueue-multiprocess"
    root.mkdir(parents=True, exist_ok=True)
    report = BenchmarkReport(
        subject=subject(),
        environment=environment(root),
        profile=profile_metadata(config),
        run={"status": "running"},
    )
    durabilities = (config.durability,) if config.durability else ("normal", "full")
    try:
        for producers, consumers, payload_bytes in multiprocess_matrix(config.profile):
            for durability in durabilities:
                raw = run_multiprocess_scenario(
                    root,
                    producers=producers,
                    consumers=consumers,
                    messages=config.messages,
                    payload_bytes=payload_bytes,
                    durability=durability,
                    timeout=config.timeout_seconds,
                    exact_id_validation=config.profile == "multiprocess-ci",
                    keep_workdir=config.keep_workdir,
                    sample_limit=config.sample_limit,
                )
                report.scenarios.append(
                    ScenarioResult(
                        scenario_id=raw["scenario_id"],
                        operation=raw["operation"],
                        parameters=raw["parameters"],
                        work_units={"messages": config.messages},
                        sqlite=raw.get("sqlite", {}),
                        warmup={},
                        measured_samples_ns=[],
                        summary=None,
                        correctness=raw["correctness"],
                        status=raw["status"],
                        multiprocess=raw,
                    )
                )
                if output is not None:
                    _atomic_write(output, report)
                if raw["status"] != "passed" or not raw["correctness"].get("ok", False):
                    report.run["status"] = "failed"
                    if output is not None:
                        _atomic_write(output, report)
                    raise BenchmarkExecutionError(
                        raw["scenario_id"], RuntimeError("scenario correctness failed")
                    )
        if config.profile == "multiprocess-release":
            raw = run_large_database_scenario(
                root,
                rows=config.large_db_rows,
                durability=config.durability or "full",
                keep_workdir=config.keep_workdir,
            )
            report.scenarios.append(
                ScenarioResult(
                    scenario_id=raw["scenario_id"],
                    operation=raw["operation"],
                    parameters=raw["parameters"],
                    work_units={"messages": config.large_db_rows},
                    sqlite=raw["sqlite"],
                    warmup={},
                    measured_samples_ns=[],
                    summary=None,
                    correctness=raw["correctness"],
                    status=raw["status"],
                    multiprocess=raw,
                )
            )
            if output is not None:
                _atomic_write(output, report)
            if raw["status"] != "passed":
                raise BenchmarkExecutionError(
                    raw["scenario_id"],
                    RuntimeError("large database correctness failed"),
                )
        report.run["status"] = "passed"
    except Exception as error:
        report.run["status"] = "failed"
        if output is not None:
            _atomic_write(output, report)
        raise BenchmarkExecutionError("multiprocess", error) from error
    if output is not None:
        _atomic_write(output, report)
    return report


def profile_metadata(config: MultiprocessConfig) -> dict[str, Any]:
    overrides = {
        **({"durability": config.durability} if config.durability else {}),
        **(
            {"large_db_rows": config.large_db_rows}
            if config.large_db_rows != 1_000_000
            else {}
        ),
        **(
            {"sample_limit": config.sample_limit} if config.sample_limit != 1000 else {}
        ),
        **(
            {"timeout_seconds": config.timeout_seconds}
            if config.timeout_seconds != 120.0
            else {}
        ),
        **(
            {"messages": config.messages}
            if config.profile == "multiprocess-release" and config.messages != 5000
            else {}
        ),
    }
    return {
        "name": config.profile,
        "canonical": config.profile == "multiprocess-release" and not overrides,
        "large_db_rows": config.large_db_rows,
        "sample_limit": config.sample_limit,
        "overrides": overrides,
        "matrix": multiprocess_matrix(config.profile),
    }
