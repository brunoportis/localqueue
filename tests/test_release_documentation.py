from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_release_notes_template_contains_required_scope_and_limits() -> None:
    text = (ROOT / "release/release-notes-template.md").read_text(encoding="utf-8")
    required = (
        "transactional SQLite/Rust core",
        "backpressure",
        "diagnostics",
        "Integrity",
        "benchmarks",
        "Storage compatibility",
        "correlation/causality",
        "bounded per-subscription concurrency",
        "timeout behavior",
        "../docs/operational-envelope.md",
        "../docs/storage-compatibility.md",
        "NFS",
        "SMB",
        "multi-host",
        "exactly-once",
        "physical power-loss",
        "Physical ARM64",
        "Release evidence",
        "Proposed public wording",
        "production-grade transactional core",
        "production-ready for documented single-host workloads",
        "validated for documented single-host workloads",
    )
    for phrase in required:
        assert phrase in text
    assert "is production-ready" not in text.lower()
    assert (
        "release candidate validated for documented single-host workloads" not in text
    )


def test_release_workflows_have_no_automatic_publication_trigger() -> None:
    wheels = (ROOT / ".github/workflows/wheels.yml").read_text(encoding="utf-8")
    release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    all_workflows = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((ROOT / ".github/workflows").glob("*.yml"))
    )
    assert "push:\n    tags:" not in wheels
    assert wheels.count("uv publish") == 1
    assert all_workflows.count("uv publish") == 1
    assert "--trusted-publishing always" in wheels
    assert "environment: pypi" in wheels
    assert "default: dry-run" in release
    assert "prepare-candidate" in release
    assert "HEAD:refs/heads/main" not in release
    assert '"uv==0.11.6"' in release
    assert "validate-uv-lock-update" in release
    assert "uv lock --check" in release
    preparation = (ROOT / "release_gate/preparation.py").read_text(encoding="utf-8")
    assert '"uv.lock"' in preparation
    assert "validate-candidate-files" in wheels
    assert '"uv==0.11.6"' in wheels


def test_every_external_action_is_pinned_to_a_full_sha() -> None:
    pattern = re.compile(r"^\s*-?\s*uses:\s+[^@\s]+@([^\s#]+)", re.MULTILINE)
    for workflow in sorted((ROOT / ".github/workflows").glob("*.yml")):
        for revision in pattern.findall(workflow.read_text(encoding="utf-8")):
            assert re.fullmatch(r"[0-9a-f]{40}", revision), (workflow, revision)


def test_release_artifact_commands_have_explicit_packaging_bootstrap() -> None:
    cli = (ROOT / "release_gate/cli.py").read_text(encoding="utf-8")
    assert "from .artifacts import" not in cli.split("def command_inventory", 1)[0]
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    candidate = (ROOT / ".github/workflows/release-candidate.yml").read_text(
        encoding="utf-8"
    )
    promotion = (ROOT / ".github/workflows/wheels.yml").read_text(encoding="utf-8")
    assert ci.index('"packaging>=24,<26"') < ci.index("release_gate.cli inventory")
    assert candidate.index(
        "Bootstrap packaging for artifact inventory"
    ) < candidate.index("release_gate.cli inventory")
    assert candidate.index(
        "Bootstrap packaging for artifact manifest validation"
    ) < candidate.index("release_gate.cli merge-inventory")
    assert candidate.index(
        "Bootstrap packaging for artifact manifest validation"
    ) < candidate.index("release_gate.cli generate-manifest")
    assert "*${tag}*aarch64.whl" not in candidate
    assert "--interpreter python3.10" not in candidate
    for tag in ("cp310", "cp311", "cp312", "cp313", "cp314"):
        assert f"steps.{tag}.outputs.python-path" in candidate
        assert f"id: {tag}" in candidate
    assert "validate_cpython_paths.py" in candidate
    assert "Explicit CPython paths (${{ matrix.os }})" in ci
    assert "macos-15-intel" in ci and "windows-latest" in ci
    assert "validate_cpython_paths.py" in ci
    assert candidate.index("wheel-job-diagnostics") < candidate.index(
        "validate-wheel-job"
    )
    first_bootstrap = promotion.index(
        "Bootstrap packaging for evidence artifact validation"
    )
    first_validation = promotion.index("release_gate.cli validate-manifest")
    assert first_bootstrap < first_validation
    promotion_bootstrap = promotion.index(
        "Bootstrap packaging for promotion artifact validation"
    )
    promotion_validation = promotion.index(
        "release_gate.cli validate-manifest", first_validation + 1
    )
    assert promotion_bootstrap < promotion_validation


def test_release_gate_runbook_documents_go_no_go_and_recovery() -> None:
    text = (ROOT / "docs/internal/release-gate.md").read_text(encoding="utf-8")
    for phrase in (
        "dry-run",
        "prepare-candidate",
        "NO-GO",
        "Human GO",
        "candidate_run_id",
        "Recovery after a partial failure",
        "editable `localqueue` entry",
        "uv lock --check",
        "confirm v1.2.0",
        "Only then close #32",
    ):
        assert phrase in text


def test_documentation_local_links_resolve() -> None:
    documents = [
        *sorted((ROOT / "docs").rglob("*.md")),
        ROOT / "release/release-notes-template.md",
        ROOT / ".github/SECURITY.md",
    ]
    link_pattern = re.compile(r"\[[^]]+\]\(([^)]+)\)")
    for document in documents:
        for target in link_pattern.findall(document.read_text(encoding="utf-8")):
            path = target.split("#", 1)[0]
            if not path or "://" in path or path.startswith("mailto:"):
                continue
            assert (document.parent / path).resolve().exists(), (document, target)
