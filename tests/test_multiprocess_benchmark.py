from pathlib import Path

import localqueue.benchmark.multiprocess as multiprocess
import pytest
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.multiprocess import (
    make_payload,
    run_multiprocess_scenario,
    validate_ids,
)
from localqueue.benchmark.multiprocess_models import MultiprocessConfig


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
