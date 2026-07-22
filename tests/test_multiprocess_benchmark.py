from pathlib import Path

import localqueue.benchmark.multiprocess as multiprocess
import pytest
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.multiprocess import (
    _cleanup_children,
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
    assert result["correctness"]["integrity"]["ok"] is True
    assert "after_preload" in result["files"]


def test_public_models_validate_and_serialize_deterministically() -> None:
    scenario = MultiprocessScenarioConfig(1, 1, 100, "normal")
    throughput = ThroughputResult(2, 2, 2, 1_000_000_000)
    large = LargeDatabaseResult(100, 100, 10, 5)
    assert scenario.to_dict()["durability"] == "normal"
    assert throughput.to_dict()["acked_per_second"] == 2.0
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
