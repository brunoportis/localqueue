from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Iterable, Mapping

from packaging.utils import (
    InvalidWheelFilename,
    canonicalize_name,
    parse_wheel_filename,
)

SDIST_RE = re.compile(r"^localqueue-(?P<version>[^-]+)\.tar\.gz$")
SMOKE_PASSED = "passed"
SMOKE_BUILT = "built-not-smoke-tested"
SMOKE_ARM64 = "artifact-validated-not-physical-smoke"
SMOKE_NOT_APPLICABLE = "not-applicable"


class ArtifactError(ValueError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _joined(values: Iterable[str]) -> str:
    return ",".join(sorted(set(values)))


def _wheel_tags(filename: str, version: str) -> tuple[str, str, str]:
    try:
        distribution, parsed_version, _build, tags = parse_wheel_filename(filename)
    except InvalidWheelFilename as error:
        raise ArtifactError(f"unsupported wheel filename: {filename}") from error
    if str(parsed_version) != version:
        raise ArtifactError(f"distribution filename version differs: {filename}")
    if canonicalize_name(str(distribution)) != "localqueue":
        raise ArtifactError(f"wheel distribution is not localqueue: {filename}")
    # A compressed tag expands into a set of Tags.  Keep every interpreter, ABI,
    # and platform component in the manifest rather than retaining only one.
    return (
        _joined(tag.interpreter for tag in tags),
        _joined(tag.abi for tag in tags),
        _joined(tag.platform for tag in tags),
    )


def build_inventory(
    paths: Iterable[Path],
    candidate_sha: str,
    version: str,
    build_job: str,
    smoke_status: str,
    *,
    smoke_passed_filenames: Iterable[str] = (),
) -> list[dict[str, object]]:
    passed = set(smoke_passed_filenames)
    entries: list[dict[str, object]] = []
    for path in sorted(paths, key=lambda item: item.name):
        source = SDIST_RE.fullmatch(path.name)
        if path.suffix == ".whl":
            python_tag, abi_tag, platform_tag = _wheel_tags(path.name, version)
            status = SMOKE_PASSED if path.name in passed else smoke_status
        elif source:
            if source.group("version") != version:
                raise ArtifactError(
                    f"distribution filename version differs: {path.name}"
                )
            python_tag = abi_tag = platform_tag = "source"
            status = SMOKE_NOT_APPLICABLE
        else:
            raise ArtifactError(f"unsupported distribution filename: {path.name}")
        entries.append(
            {
                "filename": path.name,
                "sha256": sha256(path),
                "size": path.stat().st_size,
                "package_version": version,
                "python_tag": python_tag,
                "abi_tag": abi_tag,
                "platform_tag": platform_tag,
                "source_candidate_sha": candidate_sha,
                "build_job": build_job,
                "smoke_test_status": status,
            }
        )
    if not entries:
        raise ArtifactError("no distributions found")
    unknown_passed = passed - {entry["filename"] for entry in entries}
    if unknown_passed:
        raise ArtifactError(
            f"smoke result has no matching distribution: {sorted(unknown_passed)}"
        )
    return entries


def verify_inventory(
    inventory: list[Mapping[str, object]], root: Path, version: str, candidate_sha: str
) -> None:
    filenames = [str(item.get("filename", "")) for item in inventory]
    if len(filenames) != len(set(filenames)):
        raise ArtifactError("duplicate distribution in inventory")
    for item in inventory:
        filename = str(item.get("filename", ""))
        path = root / filename
        if not path.is_file():
            raise ArtifactError(f"missing distribution: {filename}")
        if item.get("package_version") != version:
            raise ArtifactError(f"inventory version differs: {filename}")
        if item.get("source_candidate_sha") != candidate_sha:
            raise ArtifactError(f"inventory candidate SHA differs: {filename}")
        if item.get("sha256") != sha256(path):
            raise ArtifactError(f"SHA-256 differs: {filename}")
        if item.get("size") != path.stat().st_size:
            raise ArtifactError(f"size differs: {filename}")


def _tags(item: Mapping[str, object], key: str) -> set[str]:
    return {value for value in str(item[key]).split(",") if value}


def _platform_family_matches(job: str, tags: set[str]) -> bool:
    if not tags:
        return False
    if job == "linux-x86_64":
        return all(
            (tag.startswith("linux_") or tag.startswith("manylinux"))
            and tag.endswith("_x86_64")
            for tag in tags
        )
    if job == "linux-aarch64":
        return all(
            (tag.startswith("linux_") or tag.startswith("manylinux"))
            and tag.endswith("_aarch64")
            for tag in tags
        )
    if job == "macos-x86_64":
        return all(
            tag.startswith("macosx_") and tag.endswith("_x86_64") for tag in tags
        )
    if job == "macos-arm64":
        return all(tag.startswith("macosx_") and tag.endswith("_arm64") for tag in tags)
    return job == "windows-x86_64" and tags == {"win_amd64"}


def validate_distribution_matrix(inventory: list[Mapping[str, object]]) -> None:
    wheels = [item for item in inventory if str(item["filename"]).endswith(".whl")]
    sdists = [item for item in inventory if str(item["filename"]).endswith(".tar.gz")]
    if len(sdists) != 1:
        raise ArtifactError(f"expected exactly one sdist, found {len(sdists)}")
    expected_python = {"cp310", "cp311", "cp312", "cp313", "cp314"}
    expected_jobs = {
        "linux-x86_64": {
            SMOKE_PASSED: {"cp313"},
            SMOKE_BUILT: expected_python - {"cp313"},
        },
        "linux-aarch64": {SMOKE_ARM64: expected_python},
        "macos-x86_64": {
            SMOKE_PASSED: {"cp313"},
            SMOKE_BUILT: expected_python - {"cp313"},
        },
        "macos-arm64": {
            SMOKE_PASSED: {"cp313"},
            SMOKE_BUILT: expected_python - {"cp313"},
        },
        "windows-x86_64": {
            SMOKE_PASSED: {"cp313"},
            SMOKE_BUILT: expected_python - {"cp313"},
        },
    }
    if len(wheels) != 25:
        raise ArtifactError(f"expected 25 wheels, found {len(wheels)}")
    observed: dict[str, dict[str, Mapping[str, object]]] = {
        job: {} for job in expected_jobs
    }
    for item in wheels:
        job = str(item["build_job"])
        if job not in expected_jobs:
            raise ArtifactError(f"unexpected wheel build job: {job}")
        filename = str(item["filename"])
        python_tag_value, abi_tag_value, platform_tag_value = _wheel_tags(
            filename, str(item["package_version"])
        )
        if (
            item.get("python_tag") != python_tag_value
            or item.get("abi_tag") != abi_tag_value
            or item.get("platform_tag") != platform_tag_value
        ):
            raise ArtifactError(
                f"wheel inventory tags differ from filename: {filename}"
            )
        python_tags = _tags(item, "python_tag")
        if len(python_tags) != 1 or next(iter(python_tags)) not in expected_python:
            raise ArtifactError(
                f"wheel must identify exactly one expected CPython tag: {item['filename']}"
            )
        python_tag = next(iter(python_tags))
        abi_tags = _tags(item, "abi_tag")
        if abi_tags != {python_tag}:
            raise ArtifactError(f"wheel ABI does not match interpreter: {filename}")
        platform_tags = _tags(item, "platform_tag")
        if not _platform_family_matches(job, platform_tags):
            raise ArtifactError(f"wheel platform does not match {job}: {filename}")
        if python_tag in observed[job]:
            raise ArtifactError(f"duplicate {job} wheel for {python_tag}")
        observed[job][python_tag] = item
    for job, status_policy in expected_jobs.items():
        if set(observed[job]) != expected_python:
            raise ArtifactError(f"wheel matrix is incomplete for {job}")
        for status, expected_tags in status_policy.items():
            actual_tags = {
                tag
                for tag, item in observed[job].items()
                if item.get("smoke_test_status") == status
            }
            if actual_tags != expected_tags:
                raise ArtifactError(
                    f"wheel smoke status is not truthful for {job}: {status}"
                )
