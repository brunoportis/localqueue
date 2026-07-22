from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from compatibility.run_matrix import (
    MatrixError,
    load_manifest,
    sanitize,
    sha256,
    write_json_atomic,
)


def test_manifest_is_parseable_and_unique() -> None:
    manifest = load_manifest()
    assert [item["version"] for item in manifest["baseline"]] == [
        "1.0.0",
        "1.0.1",
        "1.1.0",
        "1.1.1",
        "1.1.2",
    ]


def test_manifest_rejects_duplicate_versions(tmp_path: Path) -> None:
    manifest = (Path("compatibility/baselines.toml").read_text()).replace(
        'version = "1.0.1"', 'version = "1.0.0"'
    )
    path = tmp_path / "baselines.toml"
    path.write_text(manifest)
    with pytest.raises(MatrixError, match="duplicate"):
        load_manifest(path)


def test_manifest_rejects_tag_or_wheel_tag_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "baselines.toml"
    path.write_text(
        Path("compatibility/baselines.toml")
        .read_text()
        .replace('release_tag = "v1.0.0"', 'release_tag = "wrong"')
    )
    with pytest.raises(MatrixError, match="release tag"):
        load_manifest(path)


def test_sha256_and_atomic_json(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"wheel")
    assert sha256(artifact) == hashlib.sha256(b"wheel").hexdigest()
    output = tmp_path / "report.json"
    write_json_atomic(output, {"status": "failed", "unicode": "olá"})
    assert json.loads(output.read_text())["unicode"] == "olá"
    assert not list(tmp_path.glob(".*.tmp"))


def test_sanitize_hides_checkout_path() -> None:
    assert "<checkout>" in sanitize(str(Path.cwd() / "compatibility/run_matrix.py"))
