from __future__ import annotations

import re
from pathlib import Path
from typing import Mapping

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
REF_RE = re.compile(r"^release-candidate/v(?P<version>[0-9]+\.[0-9]+\.[0-9]+)$")


class IdentityError(ValueError):
    pass


def _section_version(path: Path, section: str) -> str:
    """Read the sole version field needed by the release gate without a runtime dep."""
    document = path.read_text(encoding="utf-8")
    section_match = re.search(
        rf"(?ms)^\[{re.escape(section)}\]\s*$\n(.*?)(?=^\[|\Z)", document
    )
    if section_match is None:
        raise IdentityError(f"{path} has no [{section}] section")
    version_match = re.search(
        r'^version\s*=\s*["\']([^"\']+)["\']\s*$', section_match.group(1), re.M
    )
    if version_match is None:
        raise IdentityError(f"{path} has no version in [{section}]")
    return version_match.group(1)


def _lock_versions(path: Path, package_name: str = "localqueue") -> list[str]:
    packages = re.findall(
        r"(?ms)^\[\[package\]\]\s*$\n(.*?)(?=^\[\[package\]\]|\Z)",
        path.read_text(encoding="utf-8"),
    )
    versions: list[str] = []
    for package in packages:
        name = re.search(r'^name\s*=\s*["\']([^"\']+)["\']\s*$', package, re.M)
        version = re.search(r'^version\s*=\s*["\']([^"\']+)["\']\s*$', package, re.M)
        if name and name.group(1) == package_name and version:
            versions.append(version.group(1))
    return versions


def _without_localqueue_package(path: Path) -> str:
    document = path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'(?ms)^\[\[package\]\]\s*$\n(?=name\s*=\s*"localqueue"\s*$).*?(?=^\[\[package\]\]|\Z)'
    )
    stripped, count = pattern.subn("", document)
    if count != 1:
        raise IdentityError("uv.lock must contain exactly one localqueue package")
    return stripped


def validate_uv_lock_update(before: Path, after: Path, expected_version: str) -> None:
    """Permit only the editable local package version to change in uv.lock."""
    if _without_localqueue_package(before) != _without_localqueue_package(after):
        raise IdentityError("uv.lock changed third-party dependency resolution")
    versions = _lock_versions(after)
    if versions != [expected_version]:
        raise IdentityError(
            f"uv.lock localqueue version differs from {expected_version}"
        )


def versions_from_tree(root: Path) -> dict[str, str]:
    python_version = _section_version(root / "pyproject.toml", "project")
    cargo_version = _section_version(root / "Cargo.toml", "package")
    lock_versions = _lock_versions(root / "Cargo.lock")
    if len(lock_versions) != 1:
        raise IdentityError("Cargo.lock must contain exactly one localqueue package")
    uv_versions = _lock_versions(root / "uv.lock")
    if len(uv_versions) != 1:
        raise IdentityError("uv.lock must contain exactly one localqueue package")
    versions = {
        "python": python_version,
        "cargo": cargo_version,
        "native": lock_versions[0],
        "uv": uv_versions[0],
    }
    if len(set(versions.values())) != 1:
        raise IdentityError(f"version metadata differs: {versions}")
    return versions


def validate_candidate(
    actual: Mapping[str, str],
    *,
    expected_sha: str,
    expected_version: str,
    expected_ref: str,
    expected_parent: str | None = None,
) -> None:
    if not SHA_RE.fullmatch(expected_sha) or actual.get("sha") != expected_sha:
        raise IdentityError("candidate SHA differs from the expected SHA")
    match = REF_RE.fullmatch(expected_ref)
    if not match or actual.get("ref") != expected_ref:
        raise IdentityError("candidate ref must be an exact release-candidate/* branch")
    if (
        match.group("version") != expected_version
        or actual.get("version") != expected_version
    ):
        raise IdentityError("candidate version differs from the expected version")
    if actual.get("parent_count") != "1":
        raise IdentityError("candidate must have exactly one parent")
    parent = actual.get("parent")
    if expected_parent is not None and parent != expected_parent:
        raise IdentityError("candidate parent differs from the prepared main SHA")
    if actual.get("main_sha") != parent:
        raise IdentityError("main advanced after candidate preparation")
    if actual.get("clean") != "true":
        raise IdentityError("candidate worktree is not clean")
