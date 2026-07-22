"""Named deterministic benchmark profiles."""

from __future__ import annotations

from dataclasses import replace

from localqueue.benchmark.config import BenchmarkConfig

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
