from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import localqueue.benchmark.environment as benchmark_environment
import localqueue.benchmark.runner as benchmark_runner
import pytest
from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.environment import subject
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.metrics import MetricSummary, percentile
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult
from localqueue.benchmark.profiles import get_profile
from localqueue.benchmark.runner import run_profile
from localqueue.exceptions import LocalQueueError


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
        BenchmarkConfig.from_profile("standard", durability="")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        BenchmarkConfig.from_profile("unknown")
    for field, value in (
        ("version", True),
        ("max_retries", True),
        ("batch_sizes", (True,)),
        ("fanout_sizes", (True,)),
    ):
        with pytest.raises(TypeError):
            BenchmarkConfig(**{field: value})  # type: ignore[arg-type]


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


def test_atomic_write_removes_temporary_file_after_serialization_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = BenchmarkReport({}, {}, {}, {}, [])

    def fail_dump(*args: object, **kwargs: object) -> None:
        raise ValueError("serialization failed")

    monkeypatch.setattr(benchmark_runner.json, "dump", fail_dump)
    with pytest.raises(ValueError, match="serialization failed"):
        benchmark_runner._atomic_write(tmp_path / "report.json", report)
    assert not list(tmp_path.glob(".report.json.*.tmp"))


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


def test_fanout_units_and_throughputs_have_one_definition(tmp_path: Path) -> None:
    config = BenchmarkConfig(
        name="fanout-test",
        samples=2,
        warmups=1,
        fanout_sizes=(3,),
        batch_sizes=(1,),
        scenario_order=("fanout-3",),
    )
    scenario = run_profile(config, workdir=tmp_path).scenarios[0]
    summary = scenario.summary
    assert summary is not None
    assert scenario.work_units == {
        "operations": 2,
        "messages": 2,
        "dispatches": 2,
        "deliveries": 6,
    }
    seconds = summary.total_elapsed_ns / 1_000_000_000
    assert summary.messages_per_second is None
    assert summary.dispatches_per_second == pytest.approx(2 / seconds)
    assert summary.deliveries_per_second == pytest.approx(6 / seconds)


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("boom"),
        ValueError("bad"),
        TypeError("wrong"),
        LocalQueueError("closed"),
    ],
)
def test_scenario_failures_are_wrapped_and_partial_report_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    output = tmp_path / "partial.json"

    def fail(*args: object, **kwargs: object) -> object:
        raise failure

    monkeypatch.setattr(benchmark_runner, "_scenario", fail)
    config = BenchmarkConfig(samples=1, warmups=1, scenario_order=("put",))
    with pytest.raises(BenchmarkExecutionError) as raised:
        run_profile(config, output=output, workdir=tmp_path)
    assert isinstance(raised.value.__cause__, type(failure))
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["run"]["status"] == "failed"
    assert report["scenarios"][0]["error"]["type"] == type(failure).__name__
    assert "localqueue-benchmark-" not in report["scenarios"][0]["error"]["message"]
    assert not list(tmp_path.glob("*.tmp"))


def test_eventbus_unavailable_is_an_execution_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        benchmark_runner,
        "_fanout",
        lambda *args, **kwargs: (_ for _ in ()).throw(ImportError("pydantic missing")),
    )
    config = BenchmarkConfig(samples=1, warmups=1, scenario_order=("fanout-1",))
    with pytest.raises(BenchmarkExecutionError) as raised:
        run_profile(config, output=tmp_path / "partial.json", workdir=tmp_path)
    assert isinstance(raised.value.__cause__, ImportError)


def test_report_write_failure_during_execution_is_not_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_write = benchmark_runner._atomic_write
    calls = 0

    def fail_second(path: Path, report: BenchmarkReport) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("output device failed")
        real_write(path, report)

    monkeypatch.setattr(benchmark_runner, "_atomic_write", fail_second)
    config = BenchmarkConfig(samples=1, warmups=1, scenario_order=("put",))
    with pytest.raises(BenchmarkExecutionError) as raised:
        run_profile(config, output=tmp_path / "partial.json", workdir=tmp_path)
    assert isinstance(raised.value.__cause__, OSError)


def test_final_report_write_failure_is_execution_error_and_preserves_last_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "final.json"
    real_write = benchmark_runner._atomic_write
    calls = 0

    def fail_only_final(path: Path, report: BenchmarkReport) -> None:
        nonlocal calls
        calls += 1
        if report.run["status"] == "passed":
            raise OSError(f"cannot write {path}")
        real_write(path, report)

    monkeypatch.setattr(benchmark_runner, "_atomic_write", fail_only_final)
    config = BenchmarkConfig(samples=1, warmups=1, scenario_order=("put",))
    with pytest.raises(BenchmarkExecutionError) as raised:
        run_profile(config, output=output, workdir=tmp_path)
    assert calls == 3
    assert isinstance(raised.value.__cause__, OSError)
    assert json.loads(output.read_text(encoding="utf-8"))["run"]["status"] == "running"
    assert not list(tmp_path.glob(".final.json.*.tmp"))


def test_cli_final_report_write_failure_is_exit_one_without_traceback(
    tmp_path: Path,
) -> None:
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "import localqueue.benchmark.runner as runner\n"
        "real_write = runner._atomic_write\n"
        "def fail_only_final(path, report):\n"
        "    if report.run['status'] == 'passed':\n"
        "        raise OSError(f'cannot write {path}')\n"
        "    return real_write(path, report)\n"
        "runner._atomic_write = fail_only_final\n",
        encoding="utf-8",
    )
    output = tmp_path / "custom" / "output" / "report.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "smoke",
            "--output",
            str(output),
            "--workdir",
            str(tmp_path / "work dir"),
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert str(output) not in result.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["run"]["status"] == "running"


def test_package_native_mismatch_refuses_to_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark_runner,
        "subject",
        lambda: {"package_native_versions_consistent": False},
    )
    with pytest.raises(RuntimeError, match="versions"):
        run_profile(BenchmarkConfig(scenario_order=("put",)))


def test_cli_subprocess_exit_codes_and_no_traceback(tmp_path: Path) -> None:
    output = tmp_path / "cli.json"
    success = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "smoke",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert success.returncode == 0
    assert "Traceback" not in success.stderr
    invalid_profile = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "unknown",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert invalid_profile.returncode == 2
    invalid_output = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "smoke",
            "--output",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert invalid_output.returncode == 2
    assert "Traceback" not in invalid_output.stderr
    full_output = tmp_path / "full.json"
    full = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "smoke",
            "--durability",
            "full",
            "--output",
            str(full_output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert full.returncode == 0
    full_report = json.loads(full_output.read_text(encoding="utf-8"))
    assert all(item["sqlite"]["synchronous"] == 2 for item in full_report["scenarios"])


def test_cli_scenario_value_error_is_exit_one_without_traceback(tmp_path: Path) -> None:
    sitecustomize = tmp_path / "sitecustomize.py"
    sitecustomize.write_text(
        "import localqueue.benchmark.runner as runner\n"
        "def fail(*args, **kwargs):\n"
        "    raise ValueError('scenario input is invalid')\n"
        "runner._scenario = fail\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    output = tmp_path / "failed.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "localqueue.benchmark",
            "--profile",
            "smoke",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["scenarios"][0]["error"]["type"] == "ValueError"


def test_subject_does_not_use_host_repository_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = tmp_path / "localqueue"
    other.mkdir()
    subprocess.run(["git", "init", str(other)], check=True, capture_output=True)
    monkeypatch.chdir(other)
    observed = subject()
    assert (
        observed["commit_sha"]
        != subprocess.run(
            ["git", "-C", str(other), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    )


def test_subject_uses_only_valid_environment_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sha = "a" * 40
    monkeypatch.setenv("LOCALQUEUE_COMMIT_SHA", sha)
    observed = subject()
    assert observed["commit_sha"] == sha
    assert observed["commit_source"] == "environment"
    monkeypatch.setenv("LOCALQUEUE_COMMIT_SHA", "not-a-sha")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(benchmark_environment, "_source_checkout", lambda path: None)
    observed = subject()
    assert observed["commit_sha"] is None
    assert observed["commit_source"] == "unavailable"


def test_checkout_with_localqueue_name_but_wrong_remote_is_rejected(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "localqueue"
    package_dir = checkout / "python" / "localqueue"
    package_dir.mkdir(parents=True)
    (checkout / "pyproject.toml").write_text('name = "localqueue"\n', encoding="utf-8")
    (checkout / "Cargo.toml").write_text('name = "localqueue"\n', encoding="utf-8")
    (package_dir / "localqueue.so").touch()
    subprocess.run(["git", "init", str(checkout)], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(checkout),
            "remote",
            "add",
            "origin",
            "https://example.invalid/other.git",
        ],
        check=True,
    )
    assert (
        benchmark_environment._source_checkout(str(package_dir / "localqueue.so"))
        is None
    )


@pytest.mark.parametrize(
    "remote",
    [
        "https://github.com/brunoportis/localqueue",
        "https://github.com/brunoportis/localqueue.git",
        "git@github.com:brunoportis/localqueue.git",
        "ssh://git@github.com/brunoportis/localqueue.git",
    ],
)
def test_normalize_remote_accepts_only_supported_canonical_origins(remote: str) -> None:
    assert benchmark_environment._normalize_remote(remote) == "brunoportis/localqueue"


@pytest.mark.parametrize(
    "remote",
    [
        "https://github.com/brunoportis/localqueue-malicious.git",
        "https://github.com/brunoportis/localqueue/fork.git",
        "https://evil.example/github.com/brunoportis/localqueue.git",
        "https://github.com/other/localqueue.git",
        "https://github.com/brunoportis/localqueue.git?redirect=evil",
    ],
)
def test_normalize_remote_rejects_prefix_collisions_and_other_origins(
    remote: str,
) -> None:
    assert benchmark_environment._normalize_remote(remote) is None


def test_subject_is_unavailable_when_git_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOCALQUEUE_COMMIT_SHA", raising=False)
    monkeypatch.setattr(benchmark_environment, "_command", lambda *args, **kwargs: None)
    observed = subject()
    assert observed["commit_sha"] is None
    assert observed["commit_source"] == "unavailable"
