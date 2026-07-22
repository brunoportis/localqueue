from pathlib import Path

from localqueue.benchmark.multiprocess import make_payload, run_multiprocess_scenario


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
