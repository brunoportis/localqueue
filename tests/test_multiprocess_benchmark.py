from pathlib import Path
from threading import BrokenBarrierError
from typing import Any

import localqueue.benchmark.multiprocess as multiprocess
import pytest
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.multiprocess import (
    _cleanup_children,
    _sanitize_worker_results,
    _throughput_intervals,
    _unavailable_series,
    make_payload,
    profile_metadata,
    run_large_database_scenario,
    run_multiprocess_scenario,
    validate_ids,
)
from localqueue.benchmark.multiprocess_models import (
    FileSnapshot,
    LargeDatabaseResult,
    MultiprocessConfig,
    MultiprocessScenarioConfig,
    ThroughputResult,
)
from localqueue.benchmark.render import render_markdown


class _FailingOutput:
    def __init__(self) -> None:
        self.closed = False
        self.joined = False

    def get(self, *, timeout: float) -> dict[str, Any]:
        del timeout
        raise OSError("IPC read failed")

    def close(self) -> None:
        self.closed = True

    def join_thread(self) -> None:
        self.joined = True


class _FakeProcess:
    def __init__(self, **kwargs: Any) -> None:
        del kwargs
        self.alive = False
        self.exitcode = 0

    def start(self) -> None:
        self.alive = True

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        del timeout

    def terminate(self) -> None:
        self.alive = False

    def kill(self) -> None:
        self.alive = False


class _FakeEvent:
    def set(self) -> None:
        pass

    def is_set(self) -> bool:
        return False


class _FakeBarrier:
    def __init__(self, error: BaseException | None) -> None:
        self.error = error

    def wait(self, timeout: float | None = None) -> None:
        del timeout
        if self.error is not None:
            raise self.error


class _FakeContext:
    def __init__(self, barrier_error: BaseException | None = None) -> None:
        self.output = _FailingOutput()
        self.barrier_error = barrier_error
        self.processes: list[_FakeProcess] = []

    def Queue(self) -> _FailingOutput:
        return self.output

    def Barrier(self, parties: int) -> _FakeBarrier:
        del parties
        return _FakeBarrier(self.barrier_error)

    def Event(self) -> _FakeEvent:
        return _FakeEvent()

    def Process(self, **kwargs: Any) -> _FakeProcess:
        process = _FakeProcess(**kwargs)
        self.processes.append(process)
        return process


def test_payload_has_deterministic_identity_and_size_metadata() -> None:
    value, actual = make_payload(7, 2, 100)
    assert value["id"] == "000000000007"
    assert value["producer_index"] == 2
    assert isinstance(value["created_ns"], int)
    assert actual >= 100


def test_spawn_scenario_isolated_and_drained(tmp_path: Path) -> None:
    result = run_multiprocess_scenario(
        tmp_path,
        producers=1,
        consumers=1,
        messages=4,
        payload_bytes=100,
        durability="normal",
        timeout=30,
    )
    assert result["status"] == "passed"
    assert result["correctness"]["ok"] is True
    assert result["correctness"]["integrity"]["ok"] is True
    assert result["metric_series"]["roundtrip_latency"]["sample_count"] == 4


def test_two_durabilities_do_not_reuse_workdir_state(tmp_path: Path) -> None:
    first = run_multiprocess_scenario(
        tmp_path,
        producers=1,
        consumers=1,
        messages=3,
        payload_bytes=100,
        durability="normal",
    )
    second = run_multiprocess_scenario(
        tmp_path,
        producers=1,
        consumers=1,
        messages=3,
        payload_bytes=100,
        durability="full",
    )
    assert first["throughput"]["messages_acked"] == 3
    assert second["throughput"]["messages_acked"] == 3


def test_multiprocess_config_rejects_boolean_message_count() -> None:
    with pytest.raises(ValueError, match="messages"):
        MultiprocessConfig(profile="multiprocess-ci", messages=True)  # type: ignore[arg-type]


def test_failed_scenario_stops_profile_and_preserves_partial_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "partial.json"
    monkeypatch.setattr(
        multiprocess, "multiprocess_matrix", lambda _name: ((1, 1, 100),)
    )
    monkeypatch.setattr(
        multiprocess,
        "run_multiprocess_scenario",
        lambda *args, **kwargs: {
            "scenario_id": "mp-failed",
            "operation": "multiprocess_roundtrip",
            "parameters": {},
            "correctness": {"ok": False},
            "status": "failed",
        },
    )
    with pytest.raises(BenchmarkExecutionError):
        multiprocess.run_multiprocess_profile(
            MultiprocessConfig(
                profile="multiprocess-ci", messages=1, durability="normal"
            ),
            output,
            tmp_path,
        )
    report = __import__("json").loads(output.read_text(encoding="utf-8"))
    assert report["run"]["status"] == "failed"
    assert report["scenarios"][0]["status"] == "failed"


@pytest.mark.parametrize("ids", ([0, 1], [0, 1, 1, 2], [0, 1, 3]))
def test_exact_id_validation_rejects_missing_duplicate_and_out_of_range(
    ids: list[int],
) -> None:
    assert validate_ids(ids, 3, exact=True).ok is False


def test_systematic_samples_are_ordered_by_global_message_id(tmp_path: Path) -> None:
    result = run_multiprocess_scenario(
        tmp_path,
        producers=4,
        consumers=8,
        messages=40,
        payload_bytes=100,
        durability="normal",
        timeout=30,
    )
    samples = result["metric_series"]["claim_latency"]["samples"]
    assert [sample[0] for sample in samples] == sorted(sample[0] for sample in samples)
    assert (
        result["metric_series"]["claim_latency"]["ordering_key"] == "global_message_id"
    )


def test_reduced_large_database_records_preload_and_counts(tmp_path: Path) -> None:
    result = run_large_database_scenario(
        tmp_path, rows=100, durability="normal", batch_size=17
    )
    assert result["status"] == "passed"
    assert result["large_database"]["target_rows"] == 100
    assert result["large_database"]["actual_rows"] == 100
    assert result["large_database"]["batch_size"] == 17
    assert result["large_database"]["measured_acks"] == 100
    assert result["large_database"]["final_counts"] == {
        "ready": 0,
        "processing": 0,
        "failed": 0,
    }
    assert result["correctness"]["integrity"]["ok"] is True
    assert "after_preload" in result["files"]
    markdown = render_markdown(
        {
            "schema_version": 1,
            "subject": {},
            "environment": {},
            "profile": {"name": "multiprocess-release", "canonical": False},
            "scenarios": [result],
        }
    )
    assert "target_rows | 100" in markdown
    assert "measured_acks | 100" in markdown
    assert "stats_after_preload" in markdown
    assert "integrity" in markdown


def test_public_models_validate_and_serialize_deterministically() -> None:
    scenario = MultiprocessScenarioConfig(1, 1, 100, "normal")
    throughput = ThroughputResult(2, 2, 2, 1_000_000_000)
    large = LargeDatabaseResult(100, 100, 10, 5)
    assert scenario.to_dict()["durability"] == "normal"
    assert throughput.to_dict()["acked_per_second"] == 2.0
    assert throughput.to_dict()["produced_per_second"] == 2.0
    assert throughput.to_dict()["acked_elapsed_ns"] == 1_000_000_000
    split_throughput = ThroughputResult(
        2,
        2,
        2,
        1_000_000_000,
        produced_elapsed_ns=500_000_000,
        harness_elapsed_ns=2_000_000_000,
    ).to_dict()
    assert split_throughput["produced_per_second"] == 4.0
    assert split_throughput["harness_elapsed_ns"] == 2_000_000_000
    assert large.to_dict()["target_rows"] == 100
    assert FileSnapshot(False, None).to_dict() == {
        "exists": False,
        "size_bytes": None,
    }
    with pytest.raises(ValueError, match="null size"):
        FileSnapshot(False, 0)


@pytest.mark.parametrize("limit, expected", ((1, 1), (3, 3), (10, 5)))
def test_sample_limit_is_honored(tmp_path: Path, limit: int, expected: int) -> None:
    result = run_multiprocess_scenario(
        tmp_path,
        producers=1,
        consumers=1,
        messages=5,
        payload_bytes=100,
        durability="normal",
        sample_limit=limit,
        timeout=30,
    )
    series = result["metric_series"]["claim_latency"]
    assert series["limit"] == limit
    assert series["sample_count"] == expected


def test_release_compact_validation_does_not_return_id_lists(tmp_path: Path) -> None:
    result = run_multiprocess_scenario(
        tmp_path,
        producers=1,
        consumers=1,
        messages=5,
        payload_bytes=100,
        durability="normal",
        exact_id_validation=False,
        timeout=30,
    )
    consumer = next(item for item in result["processes"] if item["role"] == "consumer")
    assert consumer["consumed_ids"] == []
    assert (
        result["correctness"]["id_validation"]["method"] == "count_sum_xor_sha256_sum"
    )


@pytest.mark.parametrize(
    "config, canonical, overrides",
    (
        (MultiprocessConfig("multiprocess-release", 5000), True, {}),
        (
            MultiprocessConfig("multiprocess-release", 5000, large_db_rows=100),
            False,
            {"large_db_rows": 100},
        ),
        (
            MultiprocessConfig("multiprocess-release", 5000, durability="normal"),
            False,
            {"durability": "normal"},
        ),
        (
            MultiprocessConfig("multiprocess-release", 5000, durability="full"),
            False,
            {"durability": "full"},
        ),
    ),
)
def test_release_canonical_metadata(
    config: MultiprocessConfig, canonical: bool, overrides: dict[str, object]
) -> None:
    metadata = profile_metadata(config)
    assert metadata["canonical"] is canonical
    assert metadata["overrides"] == overrides


def test_cleanup_after_failure_before_worker_collection(tmp_path: Path) -> None:
    run_path = tmp_path / "run"
    run_path.mkdir()

    class Process:
        alive = True

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.alive = False

        def join(self, timeout: float | None = None) -> None:
            del timeout

    class Output:
        closed = joined = False

        def close(self) -> None:
            self.closed = True

        def join_thread(self) -> None:
            self.joined = True

    process = Process()
    output = Output()
    _cleanup_children([process], output, run_path, False)
    assert process.is_alive() is False
    assert output.closed is True and output.joined is True
    assert not run_path.exists()


def test_throughput_intervals_exclude_startup_barrier_and_join_delays() -> None:
    worker_results = [
        {
            "id": "producer-0",
            "first_put_started_ns": 10_000,
            "last_put_completed_ns": 20_000,
        },
        {
            "id": "producer-1",
            "first_put_started_ns": 12_000,
            "last_put_completed_ns": 25_000,
        },
        {"id": "consumer-0", "last_ack_completed_ns": 40_000},
    ]

    # Harness startup at 1 ns and join completion at 9,000,000 ns are
    # deliberately outside the worker timestamp input.
    intervals = _throughput_intervals(worker_results)

    assert intervals == {
        "first_put_started_ns": 10_000,
        "last_put_completed_ns": 25_000,
        "last_ack_completed_ns": 40_000,
        "produced_elapsed_ns": 15_000,
        "acked_elapsed_ns": 30_000,
    }


def test_throughput_intervals_reject_missing_and_inconsistent_timestamps() -> None:
    with pytest.raises(RuntimeError, match="missing"):
        _throughput_intervals([])
    with pytest.raises(RuntimeError, match="inconsistent"):
        _throughput_intervals(
            [
                {
                    "first_put_started_ns": 20,
                    "last_put_completed_ns": 10,
                    "last_ack_completed_ns": 30,
                }
            ]
        )


def test_failed_metric_series_is_explicitly_unavailable() -> None:
    assert _unavailable_series(0, 3) == {
        "population_count": 0,
        "sample_count": 0,
        "limit": 3,
        "method": "systematic",
        "ordering_key": "global_message_id",
        "stride": 1,
        "unit": "ns",
        "samples": [],
        "summary": None,
    }


def test_profile_exception_adds_failed_current_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "partial.json"
    monkeypatch.setattr(
        multiprocess, "multiprocess_matrix", lambda _name: ((4, 8, 100_000),)
    )
    monkeypatch.setattr(
        multiprocess,
        "run_multiprocess_scenario",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError(f"broken diagnostics in {tmp_path / 'scenario-secret'}")
        ),
    )

    with pytest.raises(BenchmarkExecutionError) as captured:
        multiprocess.run_multiprocess_profile(
            MultiprocessConfig(
                profile="multiprocess-ci", messages=10, durability="full"
            ),
            output,
            tmp_path,
        )

    report = __import__("json").loads(output.read_text(encoding="utf-8"))
    failed = report["scenarios"][0]
    assert report["run"]["status"] == "failed"
    assert failed["status"] == "failed"
    assert failed["parameters"] == {
        "producers": 4,
        "consumers": 8,
        "messages": 10,
        "payload_requested_bytes": 100_000,
        "durability": "full",
    }
    assert failed["error"]["type"] == "OSError"
    serialized = output.read_text(encoding="utf-8")
    assert str(tmp_path) not in serialized
    assert "Traceback" not in serialized
    assert str(tmp_path) not in render_markdown(report)
    assert "Traceback" not in render_markdown(report)
    assert captured.value.__cause__ is not None


def test_profile_preserves_previous_scenario_before_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "partial-two.json"
    monkeypatch.setattr(
        multiprocess,
        "multiprocess_matrix",
        lambda _name: ((1, 1, 100), (4, 8, 100_000)),
    )
    calls = 0

    def run(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        del args
        calls += 1
        if calls == 2:
            raise RuntimeError("second scenario failed")
        return {
            "scenario_id": "mp-first",
            "operation": "multiprocess_roundtrip",
            "parameters": kwargs,
            "correctness": {"ok": True},
            "status": "passed",
            "sqlite": {},
        }

    monkeypatch.setattr(multiprocess, "run_multiprocess_scenario", run)

    with pytest.raises(BenchmarkExecutionError):
        multiprocess.run_multiprocess_profile(
            MultiprocessConfig(
                profile="multiprocess-ci", messages=10, durability="normal"
            ),
            output,
            tmp_path,
        )

    report = __import__("json").loads(output.read_text(encoding="utf-8"))
    assert [scenario["status"] for scenario in report["scenarios"]] == [
        "passed",
        "failed",
    ]
    assert report["scenarios"][1]["parameters"]["producers"] == 4
    assert report["run"]["status"] == "failed"


def test_worker_errors_are_structured_and_paths_are_sanitized(tmp_path: Path) -> None:
    scenario_path = tmp_path / "run" / "scenario"
    results = _sanitize_worker_results(
        [
            {
                "id": "producer-0",
                "error": OSError(f"producer could not open {scenario_path / 'db'}"),
            },
            {
                "id": "consumer-0",
                "error": {
                    "type": "ConsumerFailure",
                    "message": f"consumer failed in {scenario_path}",
                },
            },
        ],
        (tmp_path, scenario_path),
    )

    assert results[0]["error"] == {
        "type": "WorkerError",
        "message": "producer could not open <path>/db",
    }
    assert results[1]["error"] == {
        "type": "ConsumerFailure",
        "message": "consumer failed in <path>",
    }
    assert str(tmp_path) not in str(results)

    windows = _sanitize_worker_results(
        [
            {
                "id": "producer-0",
                "error": r"failed at C:\bench\run\scenario\db",
            }
        ],
        (r"C:\bench\run\scenario",),
    )
    assert windows[0]["error"]["message"] == "failed at <path>/db"


def test_ipc_oserror_after_barrier_cleans_processes_queue_and_workdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _FakeContext()
    monkeypatch.setattr(multiprocess.multiprocessing, "get_context", lambda _: context)

    with pytest.raises(OSError, match="IPC read failed"):
        run_multiprocess_scenario(
            tmp_path,
            producers=1,
            consumers=1,
            messages=1,
            payload_bytes=100,
            durability="normal",
        )

    assert context.output.closed and context.output.joined
    assert all(not process.is_alive() for process in context.processes)
    assert list(tmp_path.iterdir()) == []


def test_broken_barrier_cleans_all_scenario_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _FakeContext(BrokenBarrierError("broken barrier"))
    monkeypatch.setattr(multiprocess.multiprocessing, "get_context", lambda _: context)

    with pytest.raises(BrokenBarrierError, match="broken barrier"):
        run_multiprocess_scenario(
            tmp_path,
            producers=1,
            consumers=1,
            messages=1,
            payload_bytes=100,
            durability="normal",
        )

    assert context.output.closed and context.output.joined
    assert all(not process.is_alive() for process in context.processes)
    assert list(tmp_path.iterdir()) == []


def test_diagnostics_failure_honors_cleanup_and_keep_workdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        multiprocess,
        "_sqlite_settings",
        lambda _queue: (_ for _ in ()).throw(RuntimeError("diagnostics failed")),
    )
    with pytest.raises(RuntimeError, match="diagnostics failed"):
        run_multiprocess_scenario(
            tmp_path,
            producers=1,
            consumers=1,
            messages=1,
            payload_bytes=100,
            durability="normal",
        )
    assert list(tmp_path.iterdir()) == []

    with pytest.raises(RuntimeError, match="diagnostics failed"):
        run_multiprocess_scenario(
            tmp_path,
            producers=1,
            consumers=1,
            messages=1,
            payload_bytes=100,
            durability="normal",
            keep_workdir=True,
        )
    assert len(list(tmp_path.iterdir())) == 1


def test_metric_construction_failure_removes_scenario_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        multiprocess,
        "_series",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("metric construction failed")
        ),
    )

    with pytest.raises(RuntimeError, match="metric construction failed"):
        run_multiprocess_scenario(
            tmp_path,
            producers=1,
            consumers=1,
            messages=2,
            payload_bytes=100,
            durability="normal",
            timeout=30,
        )

    assert list(tmp_path.iterdir()) == []
