"""Named deterministic benchmark profiles."""

from __future__ import annotations

from dataclasses import replace

from localqueue.benchmark.config import BenchmarkConfig

MULTIPROCESS_CI = (
    (1, 1, 32), (4, 8, 32), (1, 1, 1024), (4, 8, 1024),
)
MULTIPROCESS_RELEASE = tuple(
    (p, c, size) for p, c in ((1, 1), (4, 1), (1, 8), (4, 8))
    for size in (100, 1024, 100_000)
)

_STANDARD = BenchmarkConfig(
    name="standard",
    warmups=3,
    samples=25,
    payload_bytes=128,
    batch_sizes=(1, 10, 100, 1000),
    fanout_sizes=(1, 10, 100),
    lease_seconds=60.0,
    max_retries=3,
    scenario_order=(
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
    ),
)
_SMOKE = replace(
    _STANDARD,
    name="smoke",
    warmups=1,
    samples=2,
    batch_sizes=(1, 10),
    fanout_sizes=(1, 10),
    scenario_order=(
        "put",
        "put_many-1",
        "put_many-10",
        "get_ack",
        "roundtrip",
        "fanout-1",
        "fanout-10",
    ),
)


def get_profile(name: str) -> BenchmarkConfig:
    if name == "standard":
        return _STANDARD
    if name == "smoke":
        return _SMOKE
    raise ValueError(f"unknown profile: {name}")

def multiprocess_matrix(name: str) -> tuple[tuple[int, int, int], ...]:
    if name == "multiprocess-ci": return MULTIPROCESS_CI
    if name == "multiprocess-release": return MULTIPROCESS_RELEASE
    raise ValueError(f"unknown profile: {name}")
