"""Validate that a release tag and all package metadata share one version."""

from __future__ import annotations

import argparse
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]


def _version_from(path: Path, key: str) -> str:
    with path.open("rb") as file:
        document = tomllib.load(file)
    section, field = key.split(".", maxsplit=1)
    return document[section][field]


def _lockfile_version() -> str:
    with (ROOT / "Cargo.lock").open("rb") as file:
        document = tomllib.load(file)
    for package in document["package"]:
        if package["name"] == "localqueue":
            return package["version"]
    raise RuntimeError("localqueue package is missing from Cargo.lock")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", required=True, help="release tag in vX.Y.Z form")
    args = parser.parse_args()

    if not args.tag.startswith("v") or len(args.tag) == 1:
        raise SystemExit(f"release tag must use vX.Y.Z form, got {args.tag!r}")

    expected = args.tag.removeprefix("v")
    versions = {
        "pyproject.toml": _version_from(ROOT / "pyproject.toml", "project.version"),
        "Cargo.toml": _version_from(ROOT / "Cargo.toml", "package.version"),
        "Cargo.lock": _lockfile_version(),
    }
    mismatches = [
        f"{name} has {version!r}"
        for name, version in versions.items()
        if version != expected
    ]
    if mismatches:
        raise SystemExit(
            f"release tag {args.tag!r} does not match package metadata: "
            + ", ".join(mismatches)
        )


if __name__ == "__main__":
    main()
