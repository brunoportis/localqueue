from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest

from release_gate.artifacts import (
    ArtifactError,
    build_inventory,
    render_wheel_build_job_diagnostics,
    validate_distribution_matrix,
    validate_wheel_build_job,
    verify_inventory,
)
from release_gate.audits import (
    AuditError,
    audit_open_issues,
    audit_release_dependencies,
    audit_security,
)
from release_gate.cli import command_wheel_job_diagnostics, is_git_ancestor
from release_gate.identity import (
    IdentityError,
    validate_candidate,
    validate_uv_lock_update,
    versions_from_tree,
)
from release_gate.manifest import ManifestError, render_summary, validate_manifest
from release_gate.preparation import (
    PreparationError,
    candidate_branch_action,
    require_campaign_version,
    validate_candidate_release_files,
    validate_workflow_run,
)
from release_gate.promotion import (
    PromotionError,
    compare_pypi_files,
    plan_git_promotion,
    pypi_files,
    reusable_release,
    validate_confirmation,
    validate_public_claim,
)
from release_gate.release_notes import (
    ReleaseNotesError,
    render_release_body,
    validate_reusable_draft_body,
)
from release_gate.reports import ReportError, validate_report, validate_report_semantics
from release_gate.simulation import (
    CommandSpy,
    PromotionState,
    simulate_promotion,
)
from scripts import validate_cpython_paths

SHA = "a" * 40
PARENT = "b" * 40
REF = "release-candidate/v1.2.0"
VERSION = "1.2.0"


def write_versions(
    root: Path,
    package: str = VERSION,
    native: str = VERSION,
    uv: str = VERSION,
) -> None:
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "localqueue"\nversion = "{package}"\n', encoding="utf-8"
    )
    (root / "Cargo.toml").write_text(
        f'[package]\nname = "localqueue"\nversion = "{native}"\n', encoding="utf-8"
    )
    (root / "Cargo.lock").write_text(
        f'version = 4\n\n[[package]]\nname = "localqueue"\nversion = "{native}"\n',
        encoding="utf-8",
    )
    (root / "uv.lock").write_text(
        f'[[package]]\nname = "localqueue"\nversion = "{uv}"\nsource = {{ editable = "." }}\n',
        encoding="utf-8",
    )


def test_versions_reject_package_native_mismatch(tmp_path: Path) -> None:
    write_versions(tmp_path, native="1.1.2")
    with pytest.raises(IdentityError, match="version metadata differs"):
        versions_from_tree(tmp_path)


def test_versions_reject_uv_lock_mismatch_and_accepts_full_identity(
    tmp_path: Path,
) -> None:
    write_versions(tmp_path, uv="1.1.2")
    with pytest.raises(IdentityError, match="version metadata differs"):
        versions_from_tree(tmp_path)
    write_versions(tmp_path)
    assert versions_from_tree(tmp_path) == {
        "python": VERSION,
        "cargo": VERSION,
        "native": VERSION,
        "uv": VERSION,
    }


def test_uv_lock_update_rejects_third_party_resolution_changes(tmp_path: Path) -> None:
    before = tmp_path / "before.lock"
    after = tmp_path / "after.lock"
    before.write_text(
        '[[package]]\nname = "dependency"\nversion = "1.0"\n\n'
        '[[package]]\nname = "localqueue"\nversion = "1.1.2"\nsource = { editable = "." }\n',
        encoding="utf-8",
    )
    after.write_text(
        '[[package]]\nname = "dependency"\nversion = "1.0"\n\n'
        '[[package]]\nname = "localqueue"\nversion = "1.2.0"\nsource = { editable = "." }\n',
        encoding="utf-8",
    )
    validate_uv_lock_update(before, after, VERSION)
    after.write_text(
        after.read_text(encoding="utf-8").replace('version = "1.0"', 'version = "1.1"'),
        encoding="utf-8",
    )
    with pytest.raises(IdentityError, match="third-party"):
        validate_uv_lock_update(before, after, VERSION)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"sha": "c" * 40}, "candidate SHA"),
        ({"version": "1.2.1"}, "candidate version"),
        ({"ref": "main"}, "release-candidate"),
        ({"parent": "c" * 40}, "candidate parent"),
        ({"main_sha": "c" * 40}, "main advanced"),
    ],
)
def test_candidate_identity_rejects_divergence(
    change: dict[str, str], message: str
) -> None:
    actual = {
        "sha": SHA,
        "version": VERSION,
        "ref": REF,
        "parent": PARENT,
        "main_sha": PARENT,
        "parent_count": "1",
        "clean": "true",
    }
    actual.update(change)
    with pytest.raises(IdentityError, match=message):
        validate_candidate(
            actual,
            expected_sha=SHA,
            expected_version=VERSION,
            expected_ref=REF,
            expected_parent=PARENT,
        )


def wheel(name: str, content: bytes = b"wheel") -> tuple[str, bytes]:
    return name, content


def test_explicit_cpython_path_validator_checks_all_paths_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    interpreters = {}
    for tag, expected in validate_cpython_paths.EXPECTED.items():
        path = tmp_path / tag
        path.write_text("placeholder", encoding="utf-8")
        interpreters[tag] = path

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        tag = Path(command[0]).name
        if command[1] == "--version":
            return subprocess.CompletedProcess(
                command,
                0,
                stdout=f"Python {validate_cpython_paths.EXPECTED[tag]}.9\n",
            )
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "implementation": "cpython",
                    "version": f"{validate_cpython_paths.EXPECTED[tag]}.9",
                    "gil_disabled": False,
                }
            ),
        )

    monkeypatch.setattr(validate_cpython_paths.subprocess, "run", run)
    results = validate_cpython_paths.validate_interpreters(interpreters)
    summary = validate_cpython_paths.render_summary(results)
    assert [result["id"] for result in results] == sorted(interpreters)
    assert "Runner platform:" in summary
    for tag, expected in validate_cpython_paths.EXPECTED.items():
        assert f"`{tag}`" in summary
        assert f"`{expected}.x`" in summary


@pytest.mark.parametrize(
    ("probe", "message"),
    [
        (
            {"implementation": "pypy", "version": "3.10.1", "gil_disabled": False},
            "not CPython",
        ),
        (
            {"implementation": "cpython", "version": "3.10.1", "gil_disabled": True},
            "free-threaded",
        ),
        (
            {"implementation": "cpython", "version": "3.11.1", "gil_disabled": False},
            "expected Python 3.10.x",
        ),
    ],
)
def test_explicit_cpython_path_validator_rejects_nonportable_interpreters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    probe: dict[str, object],
    message: str,
) -> None:
    paths = {}
    for tag in validate_cpython_paths.EXPECTED:
        path = tmp_path / tag
        path.write_text("placeholder", encoding="utf-8")
        paths[tag] = path

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        tag = Path(command[0]).name
        if command[1] == "--version":
            version = (
                "3.11.1"
                if tag == "cp310" and probe["version"] == "3.11.1"
                else f"{validate_cpython_paths.EXPECTED[tag]}.1"
            )
            return subprocess.CompletedProcess(command, 0, stdout=f"Python {version}\n")
        payload = (
            probe
            if tag == "cp310"
            else {
                "implementation": "cpython",
                "version": f"{validate_cpython_paths.EXPECTED[tag]}.1",
                "gil_disabled": False,
            }
        )
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload))

    monkeypatch.setattr(validate_cpython_paths.subprocess, "run", run)
    with pytest.raises(validate_cpython_paths.InterpreterPathError, match=message):
        validate_cpython_paths.validate_interpreters(paths)


REAL_ARM64_WHEEL_FILENAMES = (
    "localqueue-1.2.0-cp310-cp310-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    "localqueue-1.2.0-cp311-cp311-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    "localqueue-1.2.0-cp312-cp312-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    "localqueue-1.2.0-cp313-cp313-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    "localqueue-1.2.0-cp314-cp314-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
)


def arm64_inventory(tmp_path: Path) -> list[dict[str, object]]:
    paths = []
    for name in REAL_ARM64_WHEEL_FILENAMES:
        path = tmp_path / name
        path.write_bytes(name.encode())
        paths.append(path)
    return build_inventory(
        paths, SHA, VERSION, "linux-aarch64", "artifact-validated-not-physical-smoke"
    )


def test_arm64_build_job_accepts_real_compressed_manylinux_wheels(
    tmp_path: Path,
) -> None:
    inventory = arm64_inventory(tmp_path)
    validate_wheel_build_job(inventory, "linux-aarch64", VERSION)
    diagnostics = render_wheel_build_job_diagnostics(
        inventory, "linux-aarch64", SHA, VERSION
    )
    assert "manylinux_2_17_aarch64" in diagnostics
    assert "manylinux2014_aarch64" in diagnostics
    assert "cp310=1" in diagnostics and "cp314=1" in diagnostics
    assert SHA in diagnostics and VERSION in diagnostics


def test_wheel_job_diagnostics_append_to_summary_and_log(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(arm64_inventory(tmp_path)), encoding="utf-8")
    summary = tmp_path / "summary.md"
    summary.write_text("existing summary\n", encoding="utf-8")

    command_wheel_job_diagnostics(
        argparse.Namespace(
            inventory=inventory_path,
            build_job="linux-aarch64",
            candidate_sha=SHA,
            version=VERSION,
            output=summary,
        )
    )

    diagnostics = summary.read_text(encoding="utf-8")
    assert diagnostics.startswith("existing summary\n")
    assert "cp310=1" in diagnostics and "cp314=1" in diagnostics
    assert "manylinux2014_aarch64" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        (None, "expected one cp314 linux-aarch64 wheel, found 0"),
        (
            "localqueue-1.2.0-cp312-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
            "ABI does not match",
        ),
        (
            "localqueue-1.2.0-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
            "platform does not match linux-aarch64",
        ),
        (
            "localqueue-1.2.0-cp312-cp312-macosx_11_0_arm64.whl",
            "platform does not match linux-aarch64",
        ),
    ],
)
def test_arm64_build_job_rejects_invalid_or_missing_wheels(
    tmp_path: Path, replacement: str | None, message: str
) -> None:
    inventory = arm64_inventory(tmp_path)
    if replacement is None:
        inventory.pop()
    else:
        inventory = [item for item in inventory if item["python_tag"] != "cp312"]
        path = tmp_path / replacement
        path.write_bytes(replacement.encode())
        inventory.extend(
            build_inventory(
                [path],
                SHA,
                VERSION,
                "linux-aarch64",
                "artifact-validated-not-physical-smoke",
            )
        )
    with pytest.raises(ArtifactError, match=message):
        validate_wheel_build_job(inventory, "linux-aarch64", VERSION)


def test_arm64_build_job_duplicate_error_names_the_tag_and_all_observed_files(
    tmp_path: Path,
) -> None:
    inventory = arm64_inventory(tmp_path)
    duplicate = tmp_path / "localqueue-1.2.0-cp312-cp312-manylinux_2_28_aarch64.whl"
    duplicate.write_bytes(b"duplicate")
    inventory.extend(
        build_inventory(
            [duplicate],
            SHA,
            VERSION,
            "linux-aarch64",
            "artifact-validated-not-physical-smoke",
        )
    )
    with pytest.raises(ArtifactError) as error:
        validate_wheel_build_job(inventory, "linux-aarch64", VERSION)
    assert "expected one cp312 linux-aarch64 wheel, found 2" in str(error.value)
    assert duplicate.name in str(error.value)


@pytest.mark.parametrize(
    "filename",
    [
        "otherqueue-1.2.0-cp312-cp312-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
        "localqueue-1.2.1-cp312-cp312-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
    ],
)
def test_arm64_build_job_rejects_wrong_distribution_or_version(
    tmp_path: Path, filename: str
) -> None:
    path = tmp_path / filename
    path.write_bytes(b"invalid")
    with pytest.raises(ArtifactError, match="localqueue|version differs"):
        build_inventory(
            [path],
            SHA,
            VERSION,
            "linux-aarch64",
            "artifact-validated-not-physical-smoke",
        )


def test_inventory_rejects_duplicate_missing_and_wrong_version(tmp_path: Path) -> None:
    valid = "localqueue-1.2.0-cp313-cp313-manylinux_2_17_x86_64.whl"
    (tmp_path / valid).write_bytes(b"one")
    inventory = build_inventory(
        [tmp_path / valid],
        candidate_sha=SHA,
        version=VERSION,
        build_job="linux-x86_64",
        smoke_status="passed",
    )
    with pytest.raises(ArtifactError, match="duplicate"):
        verify_inventory(inventory + inventory, tmp_path, VERSION, SHA)
    (tmp_path / valid).unlink()
    with pytest.raises(ArtifactError, match="missing"):
        verify_inventory(inventory, tmp_path, VERSION, SHA)
    wrong = tmp_path / valid.replace("1.2.0", "1.2.1")
    wrong.write_bytes(b"one")
    with pytest.raises(ArtifactError, match="version"):
        build_inventory([wrong], SHA, VERSION, "linux-x86_64", "passed")


def test_inventory_rejects_hash_change_and_is_deterministic(tmp_path: Path) -> None:
    names = [
        "localqueue-1.2.0-cp314-cp314-manylinux_2_17_x86_64.whl",
        "localqueue-1.2.0-cp310-cp310-manylinux_2_17_x86_64.whl",
    ]
    for name in names:
        (tmp_path / name).write_bytes(name.encode())
    first = build_inventory(
        [tmp_path / name for name in names], SHA, VERSION, "linux-x86_64", "passed"
    )
    second = build_inventory(
        [tmp_path / name for name in reversed(names)],
        SHA,
        VERSION,
        "linux-x86_64",
        "passed",
    )
    assert first == second
    (tmp_path / names[0]).write_bytes(b"changed")
    with pytest.raises(ArtifactError, match="SHA-256"):
        verify_inventory(first, tmp_path, VERSION, SHA)


def test_inventory_parses_real_compressed_linux_and_platform_tags(
    tmp_path: Path,
) -> None:
    names = {
        "linux-x86_64": "localqueue-1.2.0-cp314-cp314-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
        "linux-aarch64": "localqueue-1.2.0-cp314-cp314-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
        "macos-x86_64": "localqueue-1.2.0-cp314-cp314-macosx_11_0_x86_64.whl",
        "macos-arm64": "localqueue-1.2.0-cp314-cp314-macosx_11_0_arm64.whl",
        "windows-x86_64": "localqueue-1.2.0-cp314-cp314-win_amd64.whl",
    }
    for job, name in names.items():
        path = tmp_path / name
        path.write_bytes(name.encode())
        item = build_inventory([path], SHA, VERSION, job, "built-not-smoke-tested")[0]
        assert item["python_tag"] == "cp314"
        assert item["abi_tag"] == "cp314"
        if job.startswith("linux"):
            assert "manylinux_2_17" in str(item["platform_tag"])
            assert "manylinux2014" in str(item["platform_tag"])


def test_inventory_rejects_wheel_from_another_distribution(tmp_path: Path) -> None:
    path = tmp_path / "otherqueue-1.2.0-cp314-cp314-manylinux_2_17_x86_64.whl"
    path.write_bytes(b"wheel")
    with pytest.raises(ArtifactError, match="not localqueue"):
        build_inventory([path], SHA, VERSION, "linux-x86_64", "built-not-smoke-tested")


def test_distribution_matrix_requires_truthful_per_wheel_smoke_status(
    tmp_path: Path,
) -> None:
    inventory: list[dict[str, object]] = []
    jobs = {
        "linux-x86_64": "manylinux_2_17_x86_64.manylinux2014_x86_64",
        "linux-aarch64": "manylinux_2_17_aarch64.manylinux2014_aarch64",
        "macos-x86_64": "macosx_11_0_x86_64",
        "macos-arm64": "macosx_11_0_arm64",
        "windows-x86_64": "win_amd64",
    }
    for job, platform in jobs.items():
        for tag in ("cp310", "cp311", "cp312", "cp313", "cp314"):
            path = tmp_path / f"localqueue-1.2.0-{tag}-{tag}-{platform}.whl"
            path.write_bytes(path.name.encode())
            default = (
                "artifact-validated-not-physical-smoke"
                if job == "linux-aarch64"
                else "built-not-smoke-tested"
            )
            inventory.extend(
                build_inventory(
                    [path],
                    SHA,
                    VERSION,
                    job,
                    default,
                    smoke_passed_filenames=[path.name]
                    if tag == "cp313" and job != "linux-aarch64"
                    else [],
                )
            )
    sdist = tmp_path / "localqueue-1.2.0.tar.gz"
    sdist.write_bytes(b"sdist")
    inventory.extend(build_inventory([sdist], SHA, VERSION, "sdist", "passed"))
    validate_distribution_matrix(inventory)
    inventory[0]["smoke_test_status"] = "passed"
    with pytest.raises(ArtifactError, match="truthful"):
        validate_distribution_matrix(inventory)


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ("localqueue-1.2.0-cp310-cp310-macosx_12_0_x86_64.whl", "platform"),
        ("localqueue-1.2.0-cp310-cp310-manylinux_2_28_aarch64.whl", "platform"),
        ("localqueue-1.2.0-cp310-abi3-manylinux_2_17_x86_64.whl", "ABI"),
    ],
)
def test_distribution_matrix_rejects_wrong_wheel_family(
    tmp_path: Path, replacement: str, message: str
) -> None:
    inventory: list[dict[str, object]] = []
    jobs = {
        "linux-x86_64": "manylinux_2_17_x86_64",
        "linux-aarch64": "manylinux_2_17_aarch64",
        "macos-x86_64": "macosx_11_0_x86_64",
        "macos-arm64": "macosx_11_0_arm64",
        "windows-x86_64": "win_amd64",
    }
    for job, platform in jobs.items():
        for tag in ("cp310", "cp311", "cp312", "cp313", "cp314"):
            filename = f"localqueue-1.2.0-{tag}-{tag}-{platform}.whl"
            if job == "linux-x86_64" and tag == "cp310":
                filename = replacement
            path = tmp_path / filename
            path.write_bytes(path.name.encode())
            default = (
                "artifact-validated-not-physical-smoke"
                if job == "linux-aarch64"
                else "built-not-smoke-tested"
            )
            inventory.extend(
                build_inventory(
                    [path],
                    SHA,
                    VERSION,
                    job,
                    default,
                    smoke_passed_filenames=[path.name]
                    if tag == "cp313" and job != "linux-aarch64"
                    else [],
                )
            )
    sdist = tmp_path / "localqueue-1.2.0.tar.gz"
    sdist.write_bytes(b"sdist")
    inventory.extend(build_inventory([sdist], SHA, VERSION, "sdist", "passed"))
    with pytest.raises(ArtifactError, match=message):
        validate_distribution_matrix(inventory)


def test_public_release_notes_renderer_is_deterministic_and_final() -> None:
    notes = (
        (Path(__file__).parents[1] / "release/release-notes-template.md")
        .read_text(encoding="utf-8")
        .replace("{{ version }}", VERSION)
    )
    claim = "production-grade transactional core"
    first = render_release_body(notes, claim, VERSION)
    assert first == render_release_body(notes, claim, VERSION)
    assert first.count("## Approved public claim") == 1
    assert claim in first
    assert "production-ready for documented single-host workloads" not in first
    assert "candidate" not in first.lower()
    assert "will be attached" not in first.lower()
    assert "after successful" not in first.lower()
    assert "proposed public wording" not in first.lower()
    assert "are attached to this GitHub Release" in first
    assert "../docs/" not in first
    assert (
        "https://github.com/brunoportis/localqueue/blob/v1.2.0/docs/operational-envelope.md"
        in first
    )
    validate_reusable_draft_body(first, first)
    with pytest.raises(ReleaseNotesError, match="differs"):
        validate_reusable_draft_body(first + "changed", first)


def test_report_rejects_other_commit_version_missing_sha_and_checkout_import() -> None:
    report = {
        "subject": {
            "candidate_sha": SHA,
            "package_version": VERSION,
            "native_version": VERSION,
            "candidate_ref": REF,
            "installed_module_path": "/tmp/venv/site-packages/localqueue/__init__.py",
        },
        "status": "passed",
    }
    validate_report(report, SHA, VERSION, REF, require_wheel=True)
    for field, value in (
        ("candidate_sha", PARENT),
        ("package_version", "1.1.2"),
        ("candidate_sha", ""),
    ):
        altered = json.loads(json.dumps(report))
        altered["subject"][field] = value
        with pytest.raises(ReportError):
            validate_report(altered, SHA, VERSION, REF, require_wheel=True)
    report["subject"]["installed_module_path"] = (
        "/checkout/python/localqueue/__init__.py"
    )
    with pytest.raises(ReportError, match="site-packages"):
        validate_report(report, SHA, VERSION, REF, require_wheel=True)
    report["subject"]["installed_module_path"] = (
        r"C:\venv\Lib\site-packages\localqueue\__init__.py"
    )
    validate_report(report, SHA, VERSION, REF, require_wheel=True)


def test_semantic_report_gate_rejects_reduced_soak_and_truncated_crash() -> None:
    soak = {
        "success": True,
        "database": {"integrity": "ok"},
        "configuration": {
            "duration_seconds": 30.0,
            "messages": 1_000,
            "producers": 4,
            "consumers": 8,
            "crash_rate": 0.01,
        },
    }
    with pytest.raises(ReportError, match="scheduled release profile"):
        validate_report_semantics("multiprocess-soak.json", soak)
    with pytest.raises(ReportError, match="scenario catalog"):
        validate_report_semantics(
            "deterministic-crash.json",
            {"scenarios": [{"scenario": "control-ack", "status": "passed"}]},
        )


def issue(
    number: int, body: str, title: str = "Issue", labels: list[str] | None = None
) -> dict[str, object]:
    return {
        "number": number,
        "title": title,
        "body": body,
        "labels": [{"name": value} for value in labels or []],
    }


def test_open_issue_audit_blocks_p0_p1_and_treats_31_as_limitation() -> None:
    with pytest.raises(AuditError, match="#99"):
        audit_open_issues([issue(99, "Priority: P1\nsilent data loss")], [])
    result = audit_open_issues(
        [
            issue(14, "Priority: P1"),
            issue(32, "Priority: P1"),
            issue(31, "Priority: P2\nphysical ARM64"),
        ],
        [],
    )
    assert result["status"] == "passed"
    assert result["limitations"][0]["issue"] == 31
    with pytest.raises(AuditError, match="#31"):
        audit_open_issues([issue(31, "Priority: P1\nsecurity blocker")], [])


def test_issue_exception_requires_rationale_evidence_and_approver() -> None:
    blocker = issue(99, "Priority: P1\ncorrectness")
    with pytest.raises(AuditError, match="invalid exception"):
        audit_open_issues([blocker], [{"issue": 99, "rationale": "later"}])


def test_release_dependencies_require_merged_ancestor_and_changelog() -> None:
    closed = {
        "number": 15,
        "state": "CLOSED",
        "closedByPullRequestsReferences": {
            "nodes": [{"merged": True, "mergeCommit": {"oid": SHA}}]
        },
    }
    result = audit_release_dependencies(
        [closed],
        [{"issue": 15, "changelog_terms": ["Pyrefly"]}],
        "Adopt strict Pyrefly checks",
        lambda commit: commit == SHA,
    )
    assert result["status"] == "passed"
    with pytest.raises(AuditError, match="no merged closing PR"):
        audit_release_dependencies(
            [closed],
            [{"issue": 15, "changelog_terms": ["Pyrefly"]}],
            "Adopt strict Pyrefly checks",
            lambda _commit: False,
        )


def git_repository(tmp_path: Path) -> tuple[Path, str, str, str]:
    root = tmp_path / "history"
    root.mkdir()

    def run(*arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    run("init", "--initial-branch=main")
    run("config", "user.name", "Release Gate Test")
    run("config", "user.email", "release-gate@example.test")
    (root / "history.txt").write_text("base\n", encoding="utf-8")
    run("add", "history.txt")
    run("commit", "-m", "base")
    base = run("rev-parse", "HEAD")
    (root / "history.txt").write_text("base\nfirst\n", encoding="utf-8")
    run("commit", "-am", "first ancestor")
    first_ancestor = run("rev-parse", "HEAD")
    (root / "history.txt").write_text("base\nfirst\nsecond\n", encoding="utf-8")
    run("commit", "-am", "second ancestor")
    candidate = run("rev-parse", "HEAD")
    run("switch", "-c", "divergent", base)
    (root / "history.txt").write_text("base\ndivergent\n", encoding="utf-8")
    run("commit", "-am", "divergent commit")
    divergent = run("rev-parse", "HEAD")
    run("switch", "main")
    return root, first_ancestor, candidate, divergent


def test_git_ancestry_distinguishes_ancestor_divergence_and_unknown_sha(
    tmp_path: Path,
) -> None:
    root, ancestor, candidate, divergent = git_repository(tmp_path)

    assert is_git_ancestor(ancestor, candidate, root) is True
    assert is_git_ancestor(divergent, candidate, root) is False

    unknown = "f" * 40
    with pytest.raises(AuditError) as error:
        is_git_ancestor(unknown, candidate, root)
    message = str(error.value)
    assert unknown in message
    assert candidate in message
    assert "git exited 128" in message
    assert "Not a valid commit name" in message


def test_dependency_audit_fails_for_known_divergent_commit_and_passes_ancestors(
    tmp_path: Path,
) -> None:
    root, first_ancestor, candidate, divergent = git_repository(tmp_path)

    def closed(number: int, commit: str) -> dict[str, object]:
        return {
            "number": number,
            "state": "CLOSED",
            "closedByPullRequestsReferences": {
                "nodes": [{"merged": True, "mergeCommit": {"oid": commit}}]
            },
        }

    def is_ancestor(commit: str) -> bool:
        return is_git_ancestor(commit, candidate, root)

    with pytest.raises(AuditError, match="no merged closing PR"):
        audit_release_dependencies(
            [closed(15, divergent)],
            [{"issue": 15, "changelog_terms": ["history"]}],
            "history",
            is_ancestor,
        )

    result = audit_release_dependencies(
        [closed(15, first_ancestor), closed(17, candidate)],
        [
            {"issue": 15, "changelog_terms": ["first"]},
            {"issue": 17, "changelog_terms": ["second"]},
        ],
        "first and second",
        is_ancestor,
    )
    assert result["status"] == "passed"


def test_security_unknown_requires_manual_confirmation(tmp_path: Path) -> None:
    policy = tmp_path / "SECURITY.md"
    policy.write_text(
        "[Private reporting](https://github.com/acme/repo/security/advisories/new)\n",
        encoding="utf-8",
    )
    result = audit_security(
        policy, private_reporting=None, gitleaks=True, cargo_deny=True
    )
    assert result["private_vulnerability_reporting"] == "manual_confirmation_required"
    assert result["status"] == "manual_confirmation_required"


def minimal_manifest(tmp_path: Path) -> tuple[dict[str, object], Path]:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    inventory: list[dict[str, object]] = []
    platforms = {
        "linux-x86_64": "manylinux_2_17_x86_64",
        "linux-aarch64": "manylinux_2_17_aarch64",
        "macos-x86_64": "macosx_10_12_x86_64",
        "macos-arm64": "macosx_11_0_arm64",
        "windows-x86_64": "win_amd64",
    }
    for python_tag in ("cp310", "cp311", "cp312", "cp313", "cp314"):
        for job, platform in platforms.items():
            wheel_path = bundle / (
                f"localqueue-1.2.0-{python_tag}-{python_tag}-{platform}.whl"
            )
            wheel_path.write_bytes(wheel_path.name.encode())
            default = (
                "artifact-validated-not-physical-smoke"
                if job == "linux-aarch64"
                else "built-not-smoke-tested"
            )
            inventory.extend(
                build_inventory(
                    [wheel_path],
                    SHA,
                    VERSION,
                    job,
                    default,
                    smoke_passed_filenames=[wheel_path.name]
                    if python_tag == "cp313" and job != "linux-aarch64"
                    else [],
                )
            )
    distribution = bundle / "localqueue-1.2.0.tar.gz"
    distribution.write_bytes(b"sdist")
    inventory.extend(build_inventory([distribution], SHA, VERSION, "sdist", "passed"))
    report = bundle / "ci-summary.json"
    report.write_text(
        json.dumps(
            {
                "status": "passed",
                "subject": {
                    "candidate_sha": SHA,
                    "package_version": VERSION,
                    "native_version": VERSION,
                    "candidate_ref": REF,
                },
            }
        ),
        encoding="utf-8",
    )
    manifest: dict[str, object] = {
        "schema_version": 1,
        "repository": "brunoportis/localqueue",
        "candidate_version": VERSION,
        "candidate_sha": SHA,
        "candidate_parent_main_sha": PARENT,
        "candidate_ref": REF,
        "created_at": "2026-07-22T00:00:00Z",
        "workflow": {
            "run_id": 42,
            "url": "https://github.com/x/actions/runs/42",
            "actor": "maintainer",
        },
        "versions": {
            "python": VERSION,
            "cargo": VERSION,
            "native": VERSION,
            "uv": VERSION,
        },
        "distributions": inventory,
        "reports": [
            {
                "path": report.name,
                "sha256": __import__("hashlib").sha256(report.read_bytes()).hexdigest(),
            }
        ],
        "required_jobs": {"ci": "passed"},
        "skipped_scenarios": [{"name": "physical-arm64", "reason": "tracked by #31"}],
        "supported_platforms": ["linux-x86_64"],
        "known_limitations": ["No physical ARM64 or power-loss evidence"],
        "open_issue_audit": {"status": "passed", "blockers": []},
        "security_audit": {
            "status": "passed",
            "private_vulnerability_reporting": "enabled",
        },
        "proposed_claims": [
            "production-grade transactional core",
            "production-ready for documented single-host workloads",
            "validated for documented single-host workloads",
        ],
        "selected_claim": None,
        "overall_status": "passed",
        "release_notes": {"path": "release-notes/v1.2.0.md", "sha256": "0" * 64},
        "source_documents": {
            "changelog_sha256": "1" * 64,
            "operational_envelope_sha256": "2" * 64,
            "storage_policy_sha256": "3" * 64,
            "uv_lock_sha256": "4" * 64,
        },
    }
    return manifest, bundle


def test_manifest_roundtrip_summary_and_utf8(tmp_path: Path) -> None:
    manifest, bundle = minimal_manifest(tmp_path)
    validate_manifest(
        manifest, bundle, required_reports=["ci-summary.json"], required_jobs=["ci"]
    )
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
    assert json.loads(encoded) == manifest
    summary = render_summary(manifest)
    assert "v1.2.0" in summary and "Candidate SHA" in summary
    assert "Distribution smoke status" in summary
    assert "built-not-smoke-tested" in summary


def test_manifest_enforces_versioned_schema_constants_and_extra_fields(
    tmp_path: Path,
) -> None:
    manifest, bundle = minimal_manifest(tmp_path)
    manifest["repository"] = "attacker/fork"
    with pytest.raises(ManifestError, match="repository"):
        validate_manifest(manifest, bundle, ["ci-summary.json"], ["ci"])
    manifest["repository"] = "brunoportis/localqueue"
    manifest["unexpected"] = True
    with pytest.raises(ManifestError, match="unexpected"):
        validate_manifest(manifest, bundle, ["ci-summary.json"], ["ci"])


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda m: m["reports"].clear(), "reports"),
        (lambda m: m["required_jobs"].clear(), "required job"),
        (
            lambda m: m["skipped_scenarios"].append({"name": "required", "reason": ""}),
            "reason",
        ),
    ],
)
def test_manifest_rejects_missing_evidence(
    tmp_path: Path, mutation: object, message: str
) -> None:
    manifest, bundle = minimal_manifest(tmp_path)
    mutation(manifest)  # type: ignore[operator]
    with pytest.raises(ManifestError, match=message):
        validate_manifest(manifest, bundle, ["ci-summary.json"], ["ci"])


def test_public_claim_policy_and_confirmation() -> None:
    evidence = {
        name: "passed"
        for name in (
            "ci",
            "wheels",
            "storage-compatibility",
            "canonical-benchmark",
            "soak",
            "crash",
            "chaos",
        )
    }
    limitations = [
        "NFS and SMB unsupported",
        "multi-host unsupported",
        "no exactly-once",
        "physical ARM64 unvalidated",
        "physical power loss unvalidated",
    ]
    validate_public_claim(
        "production-ready for documented single-host workloads", evidence, limitations
    )
    with pytest.raises(PromotionError, match="unsupported public claim"):
        validate_public_claim("universally production ready", evidence, [])
    evidence["chaos"] = "failed"
    with pytest.raises(PromotionError, match="requires complete evidence"):
        validate_public_claim(
            "production-ready for documented single-host workloads",
            evidence,
            limitations,
        )
    validate_confirmation("publish v1.2.0", VERSION)
    with pytest.raises(PromotionError, match="confirmation"):
        validate_confirmation("yes", VERSION)


@pytest.mark.parametrize(
    "claim",
    json.loads(
        (Path(__file__).parents[1] / "release/claims-policy.json").read_text(
            encoding="utf-8"
        )
    )["claims"],
)
def test_every_policy_claim_is_renderable_as_a_final_release(claim: str) -> None:
    policy = json.loads(
        (Path(__file__).parents[1] / "release/claims-policy.json").read_text(
            encoding="utf-8"
        )
    )
    evidence = {name: "passed" for name in policy["claims"][claim]["required_jobs"]}
    limitations = [
        "NFS and SMB unsupported",
        "multi-host unsupported",
        "no exactly-once",
        "physical ARM64 unvalidated",
        "physical power loss unvalidated",
    ]
    validate_public_claim(claim, evidence, limitations, policy)
    notes = (
        (Path(__file__).parents[1] / "release/release-notes-template.md")
        .read_text(encoding="utf-8")
        .replace("{{ version }}", VERSION)
    )
    body = render_release_body(notes, claim, VERSION)
    assert body.count("## Approved public claim") == 1
    assert "candidate" not in body.lower()


def test_git_tag_and_main_promotion_states() -> None:
    assert (
        plan_git_promotion(
            main_sha=PARENT, parent_sha=PARENT, tag_sha=None, candidate_sha=SHA
        )
        == "push"
    )
    assert (
        plan_git_promotion(
            main_sha=SHA, parent_sha=PARENT, tag_sha=SHA, candidate_sha=SHA
        )
        == "complete"
    )
    with pytest.raises(PromotionError, match="tag"):
        plan_git_promotion(PARENT, PARENT, "c" * 40, SHA)
    with pytest.raises(PromotionError, match="main advanced"):
        plan_git_promotion("c" * 40, PARENT, None, SHA)


def test_prepare_candidate_refuses_wrong_version_and_branch_collision() -> None:
    with pytest.raises(PreparationError, match="calculated version"):
        require_campaign_version("1.2.1")
    assert candidate_branch_action(None, SHA) == "push"
    assert candidate_branch_action(SHA, SHA) == "already-exists"
    with pytest.raises(PreparationError, match="different SHA"):
        candidate_branch_action(PARENT, SHA)


def test_prepare_candidate_requires_uv_lock_in_the_single_release_diff() -> None:
    files = {
        "Cargo.lock",
        "Cargo.toml",
        "CHANGELOG.md",
        "pyproject.toml",
        "release-notes/v1.2.0.md",
    }
    with pytest.raises(PreparationError, match="candidate release files differ"):
        validate_candidate_release_files(files)
    validate_candidate_release_files(files | {"uv.lock"})


def test_workflow_run_must_be_successful_and_match_sha() -> None:
    run = {
        "path": ".github/workflows/release-candidate.yml",
        "headSha": SHA,
        "status": "completed",
        "conclusion": "success",
    }
    validate_workflow_run(run, expected_sha=SHA, expected_workflow=run["path"])
    for change in ({"conclusion": "failure"}, {"headSha": PARENT}):
        altered = run | change
        with pytest.raises(PreparationError):
            validate_workflow_run(
                altered, expected_sha=SHA, expected_workflow=run["path"]
            )


def test_release_and_pypi_idempotency() -> None:
    assert reusable_release(None, VERSION) == "create-draft"
    assert reusable_release({"draft": True, "tag": "v1.2.0"}, VERSION) == "reuse-draft"
    assert (
        reusable_release({"draft": False, "tag": "v1.2.0"}, VERSION)
        == "already-published"
    )
    expected = [{"filename": "localqueue-1.2.0.tar.gz", "sha256": "a" * 64}]
    assert compare_pypi_files(expected, None) == "publish"
    assert compare_pypi_files(expected, expected) == "already-published"
    with pytest.raises(PromotionError, match="PyPI"):
        compare_pypi_files(
            expected, [{"filename": expected[0]["filename"], "sha256": "b" * 64}]
        )
    metadata = {
        "urls": [
            {
                "filename": expected[0]["filename"],
                "digests": {"sha256": expected[0]["sha256"]},
            }
        ]
    }
    assert pypi_files(metadata) == expected


def test_complete_promotion_gate_runs_against_spies_only(tmp_path: Path) -> None:
    manifest, bundle = minimal_manifest(tmp_path)
    manifest["required_jobs"] = {
        name: "passed"
        for name in (
            "ci",
            "wheels",
            "storage-compatibility",
            "canonical-benchmark",
            "soak",
            "crash",
            "chaos",
        )
    }
    manifest["known_limitations"] = [
        "NFS and SMB unsupported",
        "multi-host unsupported",
        "no exactly-once",
        "physical ARM64 unvalidated",
        "physical power loss unvalidated",
    ]
    spy = CommandSpy()
    state = PromotionState(
        run={
            "workflowName": "Release candidate evidence",
            "headSha": SHA,
            "status": "completed",
            "conclusion": "success",
        },
        main_sha=PARENT,
        parent_sha=PARENT,
        tag_sha=None,
        release=None,
        pypi=None,
    )
    operations = simulate_promotion(
        manifest=manifest,
        bundle=bundle,
        required_reports=["ci-summary.json"],
        required_jobs=list(manifest["required_jobs"]),
        state=state,
        claim="production-ready for documented single-host workloads",
        confirmation="publish v1.2.0",
        spy=spy,
    )
    assert "atomic-push-main-and-tag" in operations
    assert "trusted-publish" in operations
    assert spy.commands[-2] == ("uv", "publish", "--trusted-publishing", "always")
    assert not (tmp_path / "network-write").exists()
