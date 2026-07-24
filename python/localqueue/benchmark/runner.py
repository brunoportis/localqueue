"""Single-process benchmark runner."""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from localqueue import DeliveryPolicy, JsonSerializer, SimpleQueue
from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.environment import environment, subject
from localqueue.benchmark.errors import BenchmarkExecutionError, sanitize_error_message
from localqueue.benchmark.metrics import MetricSummary
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult

try:
    from localqueue.bus import BaseEvent

    class _BenchEvent(BaseEvent):
        event_name = "benchmark.event"
        value: int
        padding: str
except ImportError:  # EventBus remains optional until a fan-out scenario runs.
    _BenchEvent = None  # type: ignore[assignment,misc]


def _payload(size: int) -> tuple[dict[str, Any], int]:
    serializer = JsonSerializer[object]()
    base = len(serializer.dumps({"id": 0, "padding": ""}))
    padding = "x" * max(0, size - base)
    value = {"id": 0, "padding": padding}
    return value, len(serializer.dumps(value))


def _atomic_write(path: Path, report: BenchmarkReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(
                report.to_dict(), stream, ensure_ascii=False, indent=2, allow_nan=False
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _sanitize_error(
    error: BaseException, sensitive_paths: tuple[str | Path, ...] = ()
) -> dict[str, str]:
    return {
        "type": type(error).__name__,
        "message": sanitize_error_message(str(error), sensitive_paths),
    }


def _validate_paths(output: Path | None, workdir: Path | None) -> None:
    if workdir is not None:
        if workdir.exists() and not workdir.is_dir():
            raise ValueError("workdir must be a directory")
        workdir.mkdir(parents=True, exist_ok=True)
        if not os.access(workdir, os.W_OK):
            raise ValueError("workdir is not writable")
    if output is not None:
        if output.exists() and output.is_dir():
            raise ValueError("output must be a file")
        output.parent.mkdir(parents=True, exist_ok=True)
        if not os.access(output.parent, os.W_OK):
            raise ValueError("output parent is not writable")


def _scenario(
    config: BenchmarkConfig,
    scenario_id: str,
    operation: str,
    parameters: dict[str, Any],
    work_units: dict[str, int],
    workdir: Path | None,
    operation_fn: Callable[[SimpleQueue[object], Any], None],
    *,
    preload: Callable[[SimpleQueue[object], Any, int], None] | None = None,
) -> ScenarioResult:
    samples: list[int] = []
    warmup_total = 0
    sqlite: dict[str, Any] = {}
    correctness: dict[str, Any] = {}
    payload, actual_size = _payload(config.payload_bytes)
    parameters: dict[str, Any] = {
        **parameters,
        "payload_requested_bytes": config.payload_bytes,
        "payload_serialized_bytes": actual_size,
        "serializer": "localqueue.JsonSerializer",
    }
    with tempfile.TemporaryDirectory(
        prefix="localqueue-benchmark-", dir=str(workdir) if workdir else None
    ) as directory:
        queue: SimpleQueue[object] | None = None
        try:
            queue = SimpleQueue(
                directory,
                delivery=DeliveryPolicy(
                    lease_seconds=config.lease_seconds,
                    max_retries=config.max_retries,
                ),
                durability=config.durability_mode,
            )
            diagnostics = queue.diagnostics()
            sqlite = {
                "journal_mode": diagnostics.journal_mode,
                "synchronous": diagnostics.synchronous,
                "synchronous_name": "NORMAL"
                if diagnostics.synchronous == 1
                else "FULL"
                if diagnostics.synchronous == 2
                else "UNKNOWN",
                "durability_mode": diagnostics.durability_mode,
                "busy_timeout_ms": diagnostics.busy_timeout_ms,
                "page_size": diagnostics.page_size,
                "sqlite_version": diagnostics.sqlite_version,
            }
            if preload:
                preload(queue, payload, config.warmups + config.samples)
            for _ in range(config.warmups):
                started = time.perf_counter_ns()
                operation_fn(queue, payload)
                warmup_total += time.perf_counter_ns() - started
            started = time.perf_counter_ns()
            for _ in range(config.samples):
                individual = time.perf_counter_ns()
                operation_fn(queue, payload)
                samples.append(time.perf_counter_ns() - individual)
            elapsed = time.perf_counter_ns() - started
            integrity = queue.check_integrity(mode="full")
            stats = queue.stats()
            if operation in ("get_ack", "roundtrip"):
                expected_acked = config.warmups + config.samples
                expected_ready = 0
            else:
                expected_acked = 0
                expected_ready = (config.warmups + config.samples) * parameters.get(
                    "batch_size", 1
                )
            correctness = {
                "ok": integrity.ok
                and stats["processing"] == 0
                and stats["failed"] == 0
                and stats["acked"] == expected_acked
                and stats["ready"] == expected_ready,
                "stats": stats,
                "expected_acked": expected_acked,
                "expected_ready": expected_ready,
                "integrity": integrity.to_dict(),
            }
            if not correctness["ok"]:
                raise RuntimeError("scenario correctness invariant failed")
            summary = MetricSummary.from_samples(
                samples,
                elapsed,
                messages=work_units.get("messages"),
                batches=work_units.get("batches"),
            )
            return ScenarioResult(
                scenario_id,
                operation,
                parameters,
                work_units,
                sqlite,
                {
                    "count": config.warmups,
                    "total_elapsed_ns": warmup_total,
                    "unit": "sample",
                },
                samples,
                summary,
                correctness,
                "passed",
            )
        finally:
            if queue is not None:
                queue.close()


def _put(queue: SimpleQueue[object], payload: Any) -> None:
    queue.put(payload)


def _get_ack(queue: SimpleQueue[object], _payload: Any) -> None:
    queue.ack(queue.get_nowait())


def _roundtrip(queue: SimpleQueue[object], payload: Any) -> None:
    queue.put(payload)
    queue.ack(queue.get_nowait())


def _put_many(size: int) -> Callable[[SimpleQueue[object], Any], None]:
    def run(queue: SimpleQueue[object], payload: Any) -> None:
        queue.put_many([payload] * size)

    return run


def _preload(queue: SimpleQueue[object], payload: Any, count: int) -> None:
    queue.put_many([payload] * count)


def _fanout(
    config: BenchmarkConfig, subscriptions: int, workdir: Path | None
) -> ScenarioResult:
    from localqueue.bus import BusTopology, EventBus

    if _BenchEvent is None:
        raise RuntimeError("fan-out requires the benchmark extra")

    parameters: dict[str, Any] = {
        "subscriptions": subscriptions,
        "payload_requested_bytes": config.payload_bytes,
    }
    samples: list[int] = []
    warmup_total = 0
    with tempfile.TemporaryDirectory(
        prefix="localqueue-benchmark-", dir=str(workdir) if workdir else None
    ) as directory:
        topology = BusTopology(
            {f"sub-{i:03d}": [_BenchEvent] for i in range(subscriptions)}
        )
        bus = EventBus(
            directory,
            name="benchmark",
            topology=topology,
            delivery=DeliveryPolicy(
                lease_seconds=config.lease_seconds,
                max_retries=config.max_retries,
            ),
            durability=config.durability_mode,
        )
        try:
            fixed_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
            padding = "x" * max(0, config.payload_bytes - 64)
            event = _BenchEvent(
                event_id=UUID(int=0),
                event_created_at=fixed_time,
                value=0,
                padding=padding,
            )
            actual = len(bus.serialize_envelope(event))
            parameters["payload_serialized_bytes"] = actual
            parameters["serializer"] = "localqueue.bus.JsonSerializer"
            native = bus._native_queue
            if native is None:
                raise RuntimeError("event bus closed before metadata collection")
            diagnostics = native.diagnostics()
            sqlite = {
                "journal_mode": diagnostics.journal_mode,
                "synchronous": diagnostics.synchronous,
                "synchronous_name": "NORMAL"
                if diagnostics.synchronous == 1
                else "FULL"
                if diagnostics.synchronous == 2
                else "UNKNOWN",
                "durability_mode": diagnostics.durability_mode,
                "busy_timeout_ms": diagnostics.busy_timeout_ms,
                "page_size": diagnostics.page_size,
                "sqlite_version": diagnostics.sqlite_version,
            }
            for index in range(config.warmups):
                started = time.perf_counter_ns()
                bus.dispatch(
                    _BenchEvent(
                        event_id=UUID(int=index),
                        event_created_at=fixed_time,
                        value=index,
                        padding=padding,
                    )
                )
                warmup_total += time.perf_counter_ns() - started
            started = time.perf_counter_ns()
            for index in range(config.samples):
                individual = time.perf_counter_ns()
                receipt = bus.dispatch(
                    _BenchEvent(
                        event_id=UUID(int=config.warmups + index),
                        event_created_at=fixed_time,
                        value=index,
                        padding=event.padding,
                    )
                )
                samples.append(time.perf_counter_ns() - individual)
                if (
                    len(receipt.subscriptions) != subscriptions
                    or len(receipt.message_ids) != subscriptions
                ):
                    raise RuntimeError("fan-out delivery count invariant failed")
            elapsed = time.perf_counter_ns() - started
            queues = [
                bus._open_subscription_queue(name)
                for name in topology.subscription_names
            ]
            try:
                stats = [queue.stats() for queue in queues]
                delivered = sum(item["ready"] for item in stats)
                integrity = [
                    queue.check_integrity(mode="full").to_dict() for queue in queues
                ]
            finally:
                for queue in queues:
                    queue.close()
            expected = (config.warmups + config.samples) * subscriptions
            correctness = {
                "ok": delivered == expected and all(item["ok"] for item in integrity),
                "expected_deliveries": expected,
                "observed_ready": delivered,
                "integrity": integrity,
            }
            if not correctness["ok"]:
                raise RuntimeError("fan-out correctness invariant failed")
            summary = MetricSummary.from_samples(
                samples,
                elapsed,
                messages=None,
                dispatches=config.samples,
                deliveries=config.samples * subscriptions,
            )
            return ScenarioResult(
                f"fanout-{subscriptions}",
                "fanout",
                parameters,
                {
                    "operations": config.samples,
                    "messages": config.samples,
                    "dispatches": config.samples,
                    "deliveries": config.samples * subscriptions,
                },
                sqlite,
                {
                    "count": config.warmups,
                    "total_elapsed_ns": warmup_total,
                    "unit": "dispatch",
                },
                samples,
                summary,
                correctness,
                "passed",
            )
        finally:
            bus.close()


def run_profile(
    config: BenchmarkConfig,
    output: str | Path | None = None,
    workdir: str | Path | None = None,
) -> BenchmarkReport:
    """Run a configured single-process profile and optionally write JSON atomically."""
    if not isinstance(config, BenchmarkConfig):
        raise TypeError("config must be BenchmarkConfig")
    root = Path(workdir) if workdir is not None else None
    output_path = Path(output) if output is not None else None
    _validate_paths(output_path, root)
    subject_metadata = subject()
    if not subject_metadata["package_native_versions_consistent"]:
        raise RuntimeError(
            "package and native extension versions are unavailable or inconsistent"
        )
    report = BenchmarkReport(
        subject_metadata,
        environment(root or Path(tempfile.gettempdir())),
        {
            "name": config.name,
            "version": config.version,
            "durability": config.durability,
            "warmups": config.warmups,
            "samples": config.samples,
            "payload_bytes": config.payload_bytes,
            "batch_sizes": list(config.batch_sizes),
            "fanout_sizes": list(config.fanout_sizes),
            "lease_seconds": config.lease_seconds,
            "max_retries": config.max_retries,
            "scenario_order": list(config.scenario_order),
        },
        {
            "status": "running",
            "throughput_source": "external perf_counter_ns elapsed",
            "latency_source": "individual perf_counter_ns samples",
            "workdir": str(root) if root else None,
        },
        [],
    )
    if output_path:
        _atomic_write(output_path, report)
    for scenario_id in config.scenario_order:
        try:
            if scenario_id == "put":
                result = _scenario(
                    config,
                    "put",
                    "put",
                    {},
                    {"operations": config.samples, "messages": config.samples},
                    root,
                    _put,
                )
            elif scenario_id.startswith("put_many-"):
                size = int(scenario_id.split("-")[1])
                result = _scenario(
                    config,
                    scenario_id,
                    "put_many",
                    {"batch_size": size},
                    {
                        "operations": config.samples,
                        "batches": config.samples,
                        "messages": config.samples * size,
                    },
                    root,
                    _put_many(size),
                )
            elif scenario_id == "get_ack":
                result = _scenario(
                    config,
                    scenario_id,
                    "get_ack",
                    {},
                    {"operations": config.samples, "messages": config.samples},
                    root,
                    _get_ack,
                    preload=_preload,
                )
            elif scenario_id == "roundtrip":
                result = _scenario(
                    config,
                    scenario_id,
                    "roundtrip",
                    {},
                    {"operations": config.samples, "messages": config.samples},
                    root,
                    _roundtrip,
                )
            elif scenario_id.startswith("fanout-"):
                result = _fanout(config, int(scenario_id.split("-")[1]), root)
            else:
                raise ValueError(f"unknown scenario: {scenario_id}")
            report = BenchmarkReport(
                report.subject,
                report.environment,
                report.profile,
                {**report.run, "status": "running"},
                [*report.scenarios, result],
            )
            if output_path:
                _atomic_write(output_path, report)
        except Exception as error:
            result = ScenarioResult(
                scenario_id,
                scenario_id.split("-")[0],
                {},
                {},
                {},
                {"count": 0, "total_elapsed_ns": 0, "unit": "sample"},
                [],
                None,
                {"ok": False},
                "failed",
                _sanitize_error(
                    error,
                    tuple(
                        path
                        for path in (
                            root,
                            output_path,
                            output_path.parent if output_path else None,
                        )
                        if path is not None
                    ),
                ),
            )
            report = BenchmarkReport(
                report.subject,
                report.environment,
                report.profile,
                {**report.run, "status": "failed"},
                [*report.scenarios, result],
            )
            if output_path:
                try:
                    _atomic_write(output_path, report)
                except Exception as write_error:
                    raise BenchmarkExecutionError(
                        scenario_id, write_error
                    ) from write_error
            raise BenchmarkExecutionError(scenario_id, error) from error
    report = BenchmarkReport(
        report.subject,
        report.environment,
        report.profile,
        {**report.run, "status": "passed"},
        report.scenarios,
    )
    if output_path:
        try:
            _atomic_write(output_path, report)
        except Exception as error:
            raise BenchmarkExecutionError("report-finalize", error) from error
    return report
