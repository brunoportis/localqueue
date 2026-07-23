from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from .manifest import validate_manifest
from .preparation import validate_workflow_run
from .promotion import (
    compare_pypi_files,
    plan_git_promotion,
    reusable_release,
    validate_confirmation,
    validate_public_claim,
)


@dataclass
class PromotionState:
    run: Mapping[str, object]
    main_sha: str
    parent_sha: str
    tag_sha: str | None
    release: Mapping[str, object] | None
    pypi: list[Mapping[str, str]] | None
    existing_assets: Mapping[str, str] = field(default_factory=dict)


@dataclass
class CommandSpy:
    commands: list[tuple[str, ...]] = field(default_factory=list)

    def record(self, *command: str) -> None:
        self.commands.append(command)


def simulate_promotion(
    *,
    manifest: Mapping[str, object],
    bundle: Path,
    required_reports: list[str],
    required_jobs: list[str],
    state: PromotionState,
    claim: str,
    confirmation: str,
    spy: CommandSpy,
) -> list[str]:
    version = str(manifest["candidate_version"])
    candidate_sha = str(manifest["candidate_sha"])
    validate_workflow_run(
        state.run,
        expected_sha=candidate_sha,
        expected_workflow="Release candidate evidence",
    )
    validate_manifest(manifest, bundle, required_reports, required_jobs)
    validate_confirmation(confirmation, version)
    validate_public_claim(
        claim,
        manifest["required_jobs"],  # type: ignore[arg-type]
        manifest["known_limitations"],  # type: ignore[arg-type]
    )
    git_action = plan_git_promotion(
        state.main_sha, state.parent_sha, state.tag_sha, candidate_sha
    )
    release_action = reusable_release(state.release, version)
    expected_files = [
        {"filename": str(item["filename"]), "sha256": str(item["sha256"])}
        for item in manifest["distributions"]  # type: ignore[union-attr]
    ]
    pypi_action = compare_pypi_files(expected_files, state.pypi)
    report_hashes = {
        str(item["path"]): str(item["sha256"])
        for item in manifest["reports"]  # type: ignore[union-attr]
    }
    expected_assets = {
        **{item["filename"]: item["sha256"] for item in expected_files},
        **report_hashes,
    }
    for filename, digest in state.existing_assets.items():
        if expected_assets.get(filename) != digest:
            raise ValueError(f"existing release asset hash differs: {filename}")
    operations: list[str] = []
    if git_action == "push":
        operations.append("atomic-push-main-and-tag")
        spy.record("git", "push", "--atomic", candidate_sha)
    if release_action == "create-draft":
        operations.append("create-draft-release")
        spy.record("gh", "release", "create", f"v{version}", "--draft")
    for filename in sorted(set(expected_assets) - set(state.existing_assets)):
        operations.append(f"upload:{filename}")
        spy.record("gh", "release", "upload", filename)
    if pypi_action == "publish":
        operations.append("trusted-publish")
        spy.record("uv", "publish", "--trusted-publishing", "always")
    if release_action != "already-published":
        operations.append("publish-github-release")
        spy.record("gh", "release", "edit", f"v{version}", "--draft=false")
    return operations
