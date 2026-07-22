from __future__ import annotations

from collections.abc import Mapping

RELEASE_CANDIDATE_FILES = frozenset(
    {
        "Cargo.lock",
        "Cargo.toml",
        "CHANGELOG.md",
        "pyproject.toml",
        "release-notes/v1.2.0.md",
        "uv.lock",
    }
)


class PreparationError(ValueError):
    pass


def require_campaign_version(calculated: str, expected: str = "1.2.0") -> None:
    if calculated != expected:
        raise PreparationError(
            f"calculated version {calculated!r} differs from campaign version {expected!r}"
        )


def candidate_branch_action(existing_sha: str | None, candidate_sha: str) -> str:
    if existing_sha is None:
        return "push"
    if existing_sha == candidate_sha:
        return "already-exists"
    raise PreparationError("candidate branch already exists at a different SHA")


def validate_candidate_release_files(files: set[str]) -> None:
    if files != RELEASE_CANDIDATE_FILES:
        raise PreparationError(
            f"candidate release files differ: expected {sorted(RELEASE_CANDIDATE_FILES)}, got {sorted(files)}"
        )


def validate_workflow_run(
    run: Mapping[str, object], *, expected_sha: str, expected_workflow: str
) -> None:
    if (
        run.get("path") != expected_workflow
        and run.get("workflowName") != expected_workflow
    ):
        raise PreparationError("evidence run used the wrong workflow")
    if run.get("headSha") != expected_sha and run.get("head_sha") != expected_sha:
        raise PreparationError("evidence run head SHA differs")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise PreparationError("evidence workflow run failed or is incomplete")
