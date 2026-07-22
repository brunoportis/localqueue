from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping

from .artifacts import validate_distribution_matrix, verify_inventory
from .schema import SchemaError, validate_json_schema


class ManifestError(ValueError):
    pass


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_manifest(
    manifest: Mapping[str, object],
    bundle: Path,
    required_reports: Iterable[str],
    required_jobs: Iterable[str],
) -> None:
    schema_path = (
        Path(__file__).resolve().parents[1] / "release/evidence-manifest.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    try:
        validate_json_schema(manifest, schema)
    except SchemaError as error:
        raise ManifestError(f"evidence manifest schema violation: {error}") from error
    required = {
        "schema_version",
        "repository",
        "candidate_version",
        "candidate_sha",
        "candidate_parent_main_sha",
        "candidate_ref",
        "created_at",
        "workflow",
        "versions",
        "distributions",
        "reports",
        "required_jobs",
        "skipped_scenarios",
        "supported_platforms",
        "known_limitations",
        "open_issue_audit",
        "security_audit",
        "proposed_claims",
        "selected_claim",
        "overall_status",
    }
    missing = required - manifest.keys()
    if missing:
        raise ManifestError(f"manifest fields missing: {sorted(missing)}")
    if manifest["schema_version"] != 1:
        raise ManifestError("unsupported manifest schema version")
    version = str(manifest["candidate_version"])
    sha = str(manifest["candidate_sha"])
    versions = manifest["versions"]
    if not isinstance(versions, Mapping) or set(versions.values()) != {version}:
        raise ManifestError("manifest package/native versions differ")
    distributions = manifest["distributions"]
    if not isinstance(distributions, list):
        raise ManifestError("manifest distributions must be a list")
    try:
        verify_inventory(distributions, bundle, version, sha)
        validate_distribution_matrix(distributions)
    except ValueError as error:
        raise ManifestError(str(error)) from error
    reports = manifest["reports"]
    if not isinstance(reports, list):
        raise ManifestError("manifest reports must be a list")
    by_path = {
        str(item["path"]): item
        for item in reports
        if isinstance(item, Mapping) and "path" in item
    }
    for required_report in required_reports:
        if required_report not in by_path:
            raise ManifestError(f"required report is missing: {required_report}")
    for path, item in by_path.items():
        report_path = bundle / path
        if not report_path.is_file():
            raise ManifestError(f"report file is missing: {path}")
        if item.get("sha256") != _sha256(report_path):
            raise ManifestError(f"report SHA-256 differs: {path}")
    jobs = manifest["required_jobs"]
    if not isinstance(jobs, Mapping):
        raise ManifestError("required jobs must be an object")
    for job in required_jobs:
        if jobs.get(job) != "passed":
            raise ManifestError(f"required job is missing or failed: {job}")
    skips = manifest["skipped_scenarios"]
    if not isinstance(skips, list) or any(
        not isinstance(item, Mapping) or not str(item.get("reason", "")).strip()
        for item in skips
    ):
        raise ManifestError("every skip reason must be explicit")
    issue_audit = manifest["open_issue_audit"]
    if (
        not isinstance(issue_audit, Mapping)
        or issue_audit.get("status") != "passed"
        or issue_audit.get("blockers")
    ):
        raise ManifestError("open-issue audit did not pass")
    security = manifest["security_audit"]
    if not isinstance(security, Mapping) or security.get("status") not in {
        "passed",
        "manual_confirmation_required",
    }:
        raise ManifestError("security audit did not pass")
    if manifest["overall_status"] != "passed":
        raise ManifestError("manifest overall status is not passed")


def render_summary(manifest: Mapping[str, object]) -> str:
    jobs = manifest.get("required_jobs", {})
    lines = [
        f"# Release evidence: v{manifest['candidate_version']}",
        "",
        f"- Candidate SHA: `{manifest['candidate_sha']}`",
        f"- Candidate parent/main SHA: `{manifest['candidate_parent_main_sha']}`",
        f"- Candidate ref: `{manifest['candidate_ref']}`",
        f"- Overall status: **{manifest['overall_status']}**",
        "",
        "## Required jobs",
        "",
    ]
    if isinstance(jobs, Mapping):
        lines.extend(f"- {name}: `{jobs[name]}`" for name in sorted(jobs))
    distributions = manifest.get("distributions", [])
    lines.extend(["", "## Distribution smoke status", ""])
    if isinstance(distributions, list):
        lines.extend(
            f"- `{item['filename']}`: `{item['smoke_test_status']}`"
            for item in sorted(
                (entry for entry in distributions if isinstance(entry, Mapping)),
                key=lambda entry: str(entry.get("filename", "")),
            )
        )
    lines.extend(["", "## Known limitations", ""])
    lines.extend(f"- {item}" for item in manifest.get("known_limitations", []))  # type: ignore[union-attr]
    return "\n".join(lines) + "\n"
