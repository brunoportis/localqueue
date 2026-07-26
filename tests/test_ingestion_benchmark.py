from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _benchmark_module():
    path = Path(__file__).parents[1] / "benchmarks" / "ingestion_bench.py"
    spec = importlib.util.spec_from_file_location("ingestion_bench", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_ingestion_benchmark_matrix_covers_review_dimensions() -> None:
    module = _benchmark_module()
    scenarios = module.scenario_matrix(20_000)

    ingestion = [case for case in scenarios if case.operation == "ingest"]
    dispatch = [case for case in scenarios if case.operation == "dispatch"]
    assert len(ingestion) == 24
    assert {
        (
            case.identity,
            case.capacity,
            case.fanout,
            case.batch_size,
        )
        for case in ingestion
    } == {
        (identity, capacity, fanout, batch_size)
        for identity in (False, True)
        for capacity in (False, True)
        for fanout in (1, 5)
        for batch_size in (100, 1_000, 10_000)
    }
    assert {(case.identity, case.fanout) for case in dispatch} == {
        (identity, fanout) for identity in (False, True) for fanout in (1, 5)
    }


def test_transaction_percentile_uses_nearest_rank() -> None:
    module = _benchmark_module()
    assert module._percentile([0.4, 0.1, 0.3, 0.2], 0.95) == 0.4


def test_process_peak_rss_is_reported_as_bytes() -> None:
    module = _benchmark_module()
    assert module._process_peak_rss_bytes() > 0
