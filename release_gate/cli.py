from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import CAMPAIGN_VERSION, REPOSITORY
from .audits import (
    AuditError,
    audit_open_issues,
    audit_release_dependencies,
    audit_security,
)
from .identity import validate_candidate, validate_uv_lock_update, versions_from_tree
from .preparation import (
    candidate_branch_action,
    require_campaign_version,
    validate_candidate_release_files,
    validate_workflow_run,
)
from .promotion import (
    compare_pypi_files,
    pypi_files,
    validate_confirmation,
    validate_public_claim,
)
from .release_notes import render_release_body, validate_reusable_draft_body
from .reports import validate_report, validate_report_semantics

ROOT = Path(__file__).resolve().parents[1]


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*arguments: str, cwd: Path = ROOT, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", *arguments], cwd=cwd, check=check, capture_output=True, text=True
    )
    return completed.stdout.strip()


def command_versions(args: argparse.Namespace) -> None:
    versions = versions_from_tree(args.root)
    if args.expected and set(versions.values()) != {args.expected}:
        raise ValueError(f"version metadata differs from {args.expected}: {versions}")
    write_json(args.output, versions) if args.output else print(json.dumps(versions))


def command_require_version(args: argparse.Namespace) -> None:
    require_campaign_version(args.calculated, args.expected)


def command_branch_action(args: argparse.Namespace) -> None:
    print(candidate_branch_action(args.existing_sha or None, args.candidate_sha))


def command_validate_uv_lock_update(args: argparse.Namespace) -> None:
    validate_uv_lock_update(args.before, args.after, args.version)


def command_validate_candidate_files(args: argparse.Namespace) -> None:
    validate_candidate_release_files(
        {
            line.strip()
            for line in args.files.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    )


def command_generate_notes(args: argparse.Namespace) -> None:
    require_campaign_version(args.version)
    template = args.template.read_text(encoding="utf-8")
    rendered = template.replace("{{ version }}", args.version)
    if "{{" in rendered or "}}" in rendered:
        raise ValueError("unresolved release-note template placeholder")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")


def command_render_release_body(args: argparse.Namespace) -> None:
    args.output.write_text(
        render_release_body(
            args.notes.read_text(encoding="utf-8"), args.claim, args.version
        ),
        encoding="utf-8",
    )


def command_validate_release_body(args: argparse.Namespace) -> None:
    release = read_json(args.release)
    if not isinstance(release, dict) or not isinstance(release.get("body"), str):
        raise ValueError("release response has no body")
    validate_reusable_draft_body(
        release["body"], args.expected.read_text(encoding="utf-8")
    )


def command_validate_candidate(args: argparse.Namespace) -> None:
    versions = versions_from_tree(args.root)
    sha = git("rev-parse", "HEAD", cwd=args.root)
    parents = git("rev-list", "--parents", "-n", "1", "HEAD", cwd=args.root).split()
    main_sha = git("rev-parse", "origin/main", cwd=args.root)
    clean = not git("status", "--porcelain", "--untracked-files=normal", cwd=args.root)
    actual = {
        "sha": sha,
        "version": versions["python"],
        "ref": args.candidate_ref,
        "parent": parents[1] if len(parents) == 2 else "",
        "parent_count": str(len(parents) - 1),
        "main_sha": main_sha,
        "clean": str(clean).lower(),
    }
    validate_candidate(
        actual,
        expected_sha=args.expected_sha,
        expected_version=args.expected_version,
        expected_ref=args.candidate_ref,
        expected_parent=args.expected_parent,
    )
    if args.output:
        write_json(args.output, actual | {"versions": versions})


def command_inventory(args: argparse.Namespace) -> None:
    from .artifacts import build_inventory

    paths = [path for pattern in args.pattern for path in args.directory.glob(pattern)]
    entries = build_inventory(
        paths,
        args.candidate_sha,
        args.version,
        args.build_job,
        args.smoke_status,
        smoke_passed_filenames=args.smoke_passed_filename,
    )
    write_json(args.output, entries)


def command_merge_inventory(args: argparse.Namespace) -> None:
    from .artifacts import validate_distribution_matrix, verify_inventory

    entries: list[dict[str, object]] = []
    for source in sorted(args.inputs):
        value = read_json(source)
        if not isinstance(value, list):
            raise ValueError(f"inventory fragment is not a list: {source}")
        entries.extend(value)
    entries.sort(key=lambda item: str(item["filename"]))
    verify_inventory(entries, args.distributions, args.version, args.candidate_sha)
    if not args.allow_partial:
        validate_distribution_matrix(entries)
    write_json(args.output, entries)


def command_wheel_job_diagnostics(args: argparse.Namespace) -> None:
    from .artifacts import render_wheel_build_job_diagnostics

    inventory = read_json(args.inventory)
    if not isinstance(inventory, list):
        raise ValueError(f"inventory fragment is not a list: {args.inventory}")
    diagnostics = render_wheel_build_job_diagnostics(
        inventory, args.build_job, args.candidate_sha, args.version
    )
    print(diagnostics, end="")
    with args.output.open("a", encoding="utf-8") as output:
        output.write(diagnostics)


def command_validate_wheel_job(args: argparse.Namespace) -> None:
    from .artifacts import validate_wheel_build_job

    inventory = read_json(args.inventory)
    if not isinstance(inventory, list):
        raise ValueError(f"inventory fragment is not a list: {args.inventory}")
    validate_wheel_build_job(inventory, args.build_job, args.version)


def command_validate_report(args: argparse.Namespace) -> None:
    validate_report(
        read_json(args.report),
        args.candidate_sha,
        args.version,
        args.candidate_ref,
        require_wheel=args.require_wheel,
    )


def _subject(
    args: argparse.Namespace, installed_module_path: str = "not-applicable"
) -> dict[str, object]:
    return {
        "candidate_sha": args.candidate_sha,
        "package_version": args.version,
        "native_version": args.version,
        "candidate_ref": args.candidate_ref,
        "installed_module_path": installed_module_path,
    }


def command_summary_report(args: argparse.Namespace) -> None:
    report = {
        "schema_version": 1,
        "subject": _subject(args, args.installed_module_path),
        "status": args.status,
        "summary": {"name": args.name, "status": args.status},
    }
    write_json(args.output, report)


def command_stamp_report(args: argparse.Namespace) -> None:
    report = read_json(args.report)
    previous = report.get("subject", {})
    installed = (
        str(previous.get("installed_module_path", ""))
        if isinstance(previous, dict)
        else ""
    )
    if not installed and isinstance(previous, dict):
        installed = str(previous.get("localqueue", ""))
    report["subject"] = {
        **(previous if isinstance(previous, dict) else {}),
        **_subject(args, installed or args.installed_module_path),
    }
    if "status" not in report:
        if isinstance(report.get("run"), dict):
            report["status"] = report["run"].get("status")
        elif "passed" in report:
            report["status"] = "passed" if report["passed"] else "failed"
    validate_report(
        report,
        args.candidate_sha,
        args.version,
        args.candidate_ref,
        require_wheel=args.require_wheel,
    )
    write_json(args.output, report)


def command_issue_audit(args: argparse.Namespace) -> None:
    issues = read_json(args.issues)
    policy = read_json(args.exceptions)
    result = audit_open_issues(issues, policy["exceptions"])
    if args.dependency_issues:
        dependency_policy = read_json(args.dependency_policy)
        dependency_issues = read_json(args.dependency_issues)

        def is_ancestor(commit: str) -> bool:
            completed = subprocess.run(
                ["git", "merge-base", "--is-ancestor", commit, args.candidate_sha],
                cwd=args.root,
                check=False,
            )
            return completed.returncode == 0

        result["release_dependencies"] = audit_release_dependencies(
            dependency_issues,
            dependency_policy["dependencies"],
            args.changelog.read_text(encoding="utf-8"),
            is_ancestor,
        )
    result["subject"] = _subject(args)
    write_json(args.output, result)


def command_security_audit(args: argparse.Namespace) -> None:
    private = {"enabled": True, "disabled": False, "unknown": None}[
        args.private_reporting
    ]
    result = audit_security(
        args.policy,
        private_reporting=private,
        gitleaks=args.gitleaks == "passed",
        cargo_deny=args.cargo_deny == "passed",
    )
    result["subject"] = _subject(args)
    write_json(args.output, result)


def _manifest(args: argparse.Namespace) -> dict[str, object]:
    policy = read_json(args.required_evidence)
    job_results = read_json(args.job_results)
    missing_jobs = set(policy["jobs"]) - job_results.keys()
    if missing_jobs or any(
        job_results.get(name) != "success" for name in policy["jobs"]
    ):
        raise ValueError(
            f"required workflow jobs are missing or unsuccessful: {sorted(missing_jobs)}"
        )
    inventory = read_json(args.inventory)
    reports = []
    for name in policy["reports"]:
        path = args.bundle / name
        if not path.is_file():
            raise ValueError(f"required report is missing: {name}")
        reports.append({"path": name, "sha256": sha256(path)})
    notes = args.root / f"release-notes/v{args.version}.md"
    if not notes.is_file():
        raise ValueError(f"release notes are missing: {notes}")
    versions = versions_from_tree(args.root)
    issues = read_json(args.bundle / "open-issue-audit.json")
    security = read_json(args.bundle / "security-audit.json")
    created = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return {
        "schema_version": 1,
        "repository": REPOSITORY,
        "candidate_version": args.version,
        "candidate_sha": args.candidate_sha,
        "candidate_parent_main_sha": args.parent_sha,
        "candidate_ref": args.candidate_ref,
        "created_at": created,
        "workflow": {"run_id": args.run_id, "url": args.run_url, "actor": args.actor},
        "release_notes": {
            "path": str(notes.relative_to(args.root)).replace(os.sep, "/"),
            "sha256": sha256(notes),
        },
        "source_documents": {
            "changelog_sha256": sha256(args.root / "CHANGELOG.md"),
            "operational_envelope_sha256": sha256(
                args.root / "docs/operational-envelope.md"
            ),
            "storage_policy_sha256": sha256(
                args.root / "docs/storage-compatibility.md"
            ),
            "uv_lock_sha256": sha256(args.root / "uv.lock"),
        },
        "versions": versions,
        "distributions": inventory,
        "reports": reports,
        "required_jobs": {name: "passed" for name in policy["jobs"]},
        "skipped_scenarios": [
            {
                "name": "physical-arm64-smoke",
                "reason": "Linux ARM64 artifacts are emulation-validated; physical hardware remains #31",
            },
            {
                "name": "physical-power-loss",
                "reason": "Process crash evidence does not demonstrate physical power-loss behavior; tracked by #31",
            },
        ],
        "supported_platforms": [
            "Linux x86_64",
            "Linux aarch64 (artifact validation; no physical smoke)",
            "macOS x86_64",
            "macOS arm64",
            "Windows x86_64",
            "CPython 3.10-3.14",
        ],
        "known_limitations": [
            "NFS and SMB are unsupported",
            "multi-host access is unsupported",
            "exactly-once processing is not provided",
            "physical ARM64 remains unvalidated in #31",
            "physical power loss is not covered by process-crash evidence",
        ],
        "open_issue_audit": issues,
        "security_audit": security,
        "proposed_claims": [
            "production-grade transactional core",
            "production-ready for documented single-host workloads",
            "validated for documented single-host workloads",
        ],
        "selected_claim": None,
        "overall_status": "passed",
    }


def command_generate_manifest(args: argparse.Namespace) -> None:
    from .manifest import render_summary, validate_manifest

    policy = read_json(args.required_evidence)
    for name in policy["identity_reports"]:
        report = read_json(args.bundle / name)
        validate_report(
            report,
            args.candidate_sha,
            args.version,
            args.candidate_ref,
            require_wheel=name in policy["wheel_import_reports"],
        )
        validate_report_semantics(name, report)
    manifest = _manifest(args)
    validate_manifest(manifest, args.bundle, policy["reports"], policy["jobs"])
    write_json(args.output, manifest)
    args.summary.write_text(render_summary(manifest), encoding="utf-8")


def command_validate_manifest(args: argparse.Namespace) -> None:
    from .manifest import validate_manifest

    manifest = read_json(args.manifest)
    policy = read_json(args.required_evidence)
    validate_manifest(manifest, args.bundle, policy["reports"], policy["jobs"])
    if args.candidate_sha and manifest["candidate_sha"] != args.candidate_sha:
        raise ValueError("manifest candidate SHA differs from promotion input")
    if args.version and manifest["candidate_version"] != args.version:
        raise ValueError("manifest version differs from promotion input")
    if args.candidate_ref and manifest["candidate_ref"] != args.candidate_ref:
        raise ValueError("manifest candidate ref differs from promotion input")
    for name in policy["identity_reports"]:
        report = read_json(args.bundle / name)
        validate_report(
            report,
            str(manifest["candidate_sha"]),
            str(manifest["candidate_version"]),
            str(manifest["candidate_ref"]),
            require_wheel=name in policy["wheel_import_reports"],
        )
        validate_report_semantics(name, report)


def command_validate_run(args: argparse.Namespace) -> None:
    validate_workflow_run(
        read_json(args.run),
        expected_sha=args.candidate_sha,
        expected_workflow=args.workflow,
    )


def command_validate_claim(args: argparse.Namespace) -> None:
    manifest = read_json(args.manifest)
    validate_public_claim(
        args.claim,
        manifest["required_jobs"],
        manifest["known_limitations"],
        read_json(args.policy),
    )
    validate_confirmation(args.confirmation, args.version)
    if (
        manifest["security_audit"].get("private_vulnerability_reporting")
        == "manual_confirmation_required"
        and not args.private_vulnerability_reporting_confirmed
    ):
        raise ValueError("private vulnerability reporting requires human confirmation")


def command_validate_sources(args: argparse.Namespace) -> None:
    manifest = read_json(args.manifest)
    release_notes = manifest["release_notes"]
    source_documents = manifest["source_documents"]
    paths = {
        release_notes["path"]: release_notes["sha256"],
        "CHANGELOG.md": source_documents["changelog_sha256"],
        "docs/operational-envelope.md": source_documents["operational_envelope_sha256"],
        "docs/storage-compatibility.md": source_documents["storage_policy_sha256"],
        "uv.lock": source_documents["uv_lock_sha256"],
    }
    for relative, expected in paths.items():
        path = args.root / relative
        if not path.is_file() or sha256(path) != expected:
            raise ValueError(f"source document hash differs: {relative}")


def command_pypi_state(args: argparse.Namespace) -> None:
    inventory = read_json(args.inventory)
    expected = [
        {"filename": item["filename"], "sha256": item["sha256"]} for item in inventory
    ]
    actual = None if args.metadata is None else pypi_files(read_json(args.metadata))
    print(compare_pypi_files(expected, actual))


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(required=True)

    command = commands.add_parser("versions")
    command.add_argument("--root", type=Path, default=ROOT)
    command.add_argument("--expected")
    command.add_argument("--output", type=Path)
    command.set_defaults(handler=command_versions)

    command = commands.add_parser("require-version")
    command.add_argument("--calculated", required=True)
    command.add_argument("--expected", default=CAMPAIGN_VERSION)
    command.set_defaults(handler=command_require_version)

    command = commands.add_parser("branch-action")
    command.add_argument("--existing-sha")
    command.add_argument("--candidate-sha", required=True)
    command.set_defaults(handler=command_branch_action)

    command = commands.add_parser("validate-uv-lock-update")
    command.add_argument("--before", type=Path, required=True)
    command.add_argument("--after", type=Path, required=True)
    command.add_argument("--version", required=True)
    command.set_defaults(handler=command_validate_uv_lock_update)

    command = commands.add_parser("validate-candidate-files")
    command.add_argument("--files", type=Path, required=True)
    command.set_defaults(handler=command_validate_candidate_files)

    command = commands.add_parser("generate-notes")
    command.add_argument("--version", required=True)
    command.add_argument(
        "--template", type=Path, default=ROOT / "release/release-notes-template.md"
    )
    command.add_argument("--output", type=Path, required=True)
    command.set_defaults(handler=command_generate_notes)

    command = commands.add_parser("render-release-body")
    command.add_argument("--notes", type=Path, required=True)
    command.add_argument("--claim", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--output", type=Path, required=True)
    command.set_defaults(handler=command_render_release_body)

    command = commands.add_parser("validate-release-body")
    command.add_argument("--release", type=Path, required=True)
    command.add_argument("--expected", type=Path, required=True)
    command.set_defaults(handler=command_validate_release_body)

    command = commands.add_parser("validate-candidate")
    command.add_argument("--root", type=Path, default=ROOT)
    command.add_argument("--candidate-ref", required=True)
    command.add_argument("--expected-sha", required=True)
    command.add_argument("--expected-version", required=True)
    command.add_argument("--expected-parent")
    command.add_argument("--output", type=Path)
    command.set_defaults(handler=command_validate_candidate)

    command = commands.add_parser("inventory")
    command.add_argument("--directory", type=Path, required=True)
    command.add_argument("--pattern", action="append", required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--build-job", required=True)
    command.add_argument("--smoke-status", required=True)
    command.add_argument("--smoke-passed-filename", action="append", default=[])
    command.add_argument("--output", type=Path, required=True)
    command.set_defaults(handler=command_inventory)

    command = commands.add_parser("wheel-job-diagnostics")
    command.add_argument("--inventory", type=Path, required=True)
    command.add_argument("--build-job", required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--output", type=Path, required=True)
    command.set_defaults(handler=command_wheel_job_diagnostics)

    command = commands.add_parser("validate-wheel-job")
    command.add_argument("--inventory", type=Path, required=True)
    command.add_argument("--build-job", required=True)
    command.add_argument("--version", required=True)
    command.set_defaults(handler=command_validate_wheel_job)

    command = commands.add_parser("merge-inventory")
    command.add_argument("inputs", nargs="+", type=Path)
    command.add_argument("--distributions", type=Path, required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--allow-partial", action="store_true")
    command.add_argument("--output", type=Path, required=True)
    command.set_defaults(handler=command_merge_inventory)

    command = commands.add_parser("validate-report")
    command.add_argument("--report", type=Path, required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--candidate-ref", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--require-wheel", action="store_true")
    command.set_defaults(handler=command_validate_report)

    for name, handler in (
        ("summary-report", command_summary_report),
        ("stamp-report", command_stamp_report),
    ):
        command = commands.add_parser(name)
        if name == "stamp-report":
            command.add_argument("--report", type=Path, required=True)
            command.add_argument("--require-wheel", action="store_true")
        else:
            command.add_argument("--name", required=True)
            command.add_argument(
                "--status", choices=("passed", "failed"), required=True
            )
        command.add_argument("--candidate-sha", required=True)
        command.add_argument("--candidate-ref", required=True)
        command.add_argument("--version", required=True)
        command.add_argument("--installed-module-path", default="not-applicable")
        command.add_argument("--output", type=Path, required=True)
        command.set_defaults(handler=handler)

    command = commands.add_parser("issue-audit")
    command.add_argument("--issues", type=Path, required=True)
    command.add_argument(
        "--exceptions", type=Path, default=ROOT / "release/issue-exceptions.json"
    )
    command.add_argument("--output", type=Path, required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--candidate-ref", required=True)
    command.add_argument("--version", required=True)
    command.add_argument("--dependency-issues", type=Path)
    command.add_argument(
        "--dependency-policy",
        type=Path,
        default=ROOT / "release/dependency-policy.json",
    )
    command.add_argument("--root", type=Path, default=ROOT)
    command.add_argument("--changelog", type=Path, default=ROOT / "CHANGELOG.md")
    command.set_defaults(handler=command_issue_audit)

    command = commands.add_parser("security-audit")
    command.add_argument("--policy", type=Path, default=ROOT / ".github/SECURITY.md")
    command.add_argument(
        "--private-reporting", choices=("enabled", "disabled", "unknown"), required=True
    )
    command.add_argument("--gitleaks", choices=("passed", "failed"), required=True)
    command.add_argument("--cargo-deny", choices=("passed", "failed"), required=True)
    command.add_argument("--output", type=Path, required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--candidate-ref", required=True)
    command.add_argument("--version", required=True)
    command.set_defaults(handler=command_security_audit)

    for name, handler in (("generate-manifest", command_generate_manifest),):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, default=ROOT)
        command.add_argument("--bundle", type=Path, required=True)
        command.add_argument("--inventory", type=Path, required=True)
        command.add_argument("--job-results", type=Path, required=True)
        command.add_argument("--candidate-sha", required=True)
        command.add_argument("--parent-sha", required=True)
        command.add_argument("--candidate-ref", required=True)
        command.add_argument("--version", required=True)
        command.add_argument("--run-id", type=int, required=True)
        command.add_argument("--run-url", required=True)
        command.add_argument("--actor", required=True)
        command.add_argument(
            "--required-evidence",
            type=Path,
            default=ROOT / "release/required-evidence.json",
        )
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--summary", type=Path, required=True)
        command.set_defaults(handler=handler)

    command = commands.add_parser("validate-manifest")
    command.add_argument("--manifest", type=Path, required=True)
    command.add_argument("--bundle", type=Path, required=True)
    command.add_argument(
        "--required-evidence",
        type=Path,
        default=ROOT / "release/required-evidence.json",
    )
    command.add_argument("--candidate-sha")
    command.add_argument("--candidate-ref")
    command.add_argument("--version")
    command.set_defaults(handler=command_validate_manifest)

    command = commands.add_parser("validate-run")
    command.add_argument("--run", type=Path, required=True)
    command.add_argument("--candidate-sha", required=True)
    command.add_argument("--workflow", required=True)
    command.set_defaults(handler=command_validate_run)

    command = commands.add_parser("validate-claim")
    command.add_argument("--manifest", type=Path, required=True)
    command.add_argument("--claim", required=True)
    command.add_argument("--confirmation", required=True)
    command.add_argument("--version", required=True)
    command.add_argument(
        "--private-vulnerability-reporting-confirmed", action="store_true"
    )
    command.add_argument(
        "--policy", type=Path, default=ROOT / "release/claims-policy.json"
    )
    command.set_defaults(handler=command_validate_claim)

    command = commands.add_parser("validate-sources")
    command.add_argument("--manifest", type=Path, required=True)
    command.add_argument("--root", type=Path, default=ROOT)
    command.set_defaults(handler=command_validate_sources)

    command = commands.add_parser("pypi-state")
    command.add_argument("--inventory", type=Path, required=True)
    command.add_argument("--metadata", type=Path)
    command.set_defaults(handler=command_pypi_state)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        args.handler(args)
    except (AuditError, OSError, subprocess.SubprocessError, ValueError) as error:
        print(f"release gate: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
