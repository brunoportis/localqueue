from __future__ import annotations

import json
from pathlib import Path

import pytest
from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.metrics import MetricSummary, percentile
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult
from localqueue.benchmark.profiles import get_profile
from localqueue.benchmark.runner import run_profile


def test_percentile_uses_nearest_rank() -> None:
    samples = [50, 10, 90, 20, 30]
    assert percentile(samples, 0.50) == 30
    assert percentile(samples, 0.95) == 90
    assert percentile(samples, 0.99) == 90


def test_percentile_single_sample_and_empty_input() -> None:
    assert percentile([7], 0) == 7
    assert percentile([7], 1) == 7
    with pytest.raises(ValueError, match="at least one"):
        percentile([], 0.5)


def test_metric_summary_contains_integer_units_and_throughput() -> None:
    summary = MetricSummary.from_samples([10, 30, 20], 1_000_000_000, messages=6)
    assert summary.count == 3
    assert summary.total_elapsed_ns == 1_000_000_000
    assert summary.minimum_ns == 10
    assert summary.maximum_ns == 30
    assert summary.p50_ns == 20
    assert summary.p95_ns == 30
    assert summary.p99_ns == 30
    assert summary.operations_per_second == 3.0
    assert summary.messages_per_second == 6.0
    encoded = json.dumps(summary.to_dict(), allow_nan=False)
    assert "NaN" not in encoded and "Infinity" not in encoded


def test_profiles_are_known_immutable_and_deterministic() -> None:
    smoke = get_profile("smoke")
    standard = get_profile("standard")
    assert smoke.name == "smoke"
    assert standard.scenario_order == (
        "put",
        "put_many-1",
        "put_many-10",
        "put_many-100",
        "put_many-1000",
        "get_ack",
        "roundtrip",
        "fanout-1",
        "fanout-10",
        "fanout-100",
    )
    with pytest.raises((AttributeError, TypeError)):
        standard.samples = 1  # type: ignore[misc]


def test_config_rejects_bool_as_int_and_allows_durability_override() -> None:
    with pytest.raises(TypeError):
        BenchmarkConfig(samples=True)  # type: ignore[arg-type]
    config = BenchmarkConfig.from_profile("standard", durability="full")
    assert config.durability == "full"
    with pytest.raises(ValueError):
        BenchmarkConfig.from_profile("unknown")


def test_report_json_roundtrip_preserves_raw_samples_and_versions() -> None:
    scenario = ScenarioResult(
        scenario_id="put",
        operation="put",
        parameters={},
        work_units={"operations": 1, "messages": 1},
        sqlite={},
        warmup={"count": 1, "total_elapsed_ns": 5, "unit": "sample"},
        measured_samples_ns=[3, 2],
        summary=MetricSummary.from_samples([3, 2], 10, messages=2),
        correctness={"ok": True},
        status="passed",
    )
    report = BenchmarkReport(
        subject={}, environment={}, profile={}, run={}, scenarios=[scenario]
    )
    encoded = json.dumps(report.to_dict(), allow_nan=False)
    restored = json.loads(encoded)
    assert restored["schema_version"] == 1
    assert restored["profile"]["profile_schema_version"] == 1
    assert restored["scenarios"][0]["measured_samples_ns"] == [3, 2]


def test_smoke_runner_writes_atomic_unicode_output_and_checks_integrity(
    tmp_path: Path,
) -> None:
    output = tmp_path / "relatório benchmark.json"
    config = BenchmarkConfig(
        name="smoke-test",
        samples=1,
        warmups=1,
        batch_sizes=(1,),
        fanout_sizes=(1,),
        scenario_order=("put", "put_many-1", "get_ack", "roundtrip", "fanout-1"),
    )
    report = run_profile(config, output=output, workdir=tmp_path)
    assert report.run["status"] == "passed"
    assert output.exists()
    encoded = json.loads(output.read_text(encoding="utf-8"))
    assert [item["status"] for item in encoded["scenarios"]] == ["passed"] * 5
    assert all(item["correctness"]["integrity"] for item in encoded["scenarios"])
