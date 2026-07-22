"""Focused consistency checks for the public operational envelope."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
DOC = ROOT / "docs/operational-envelope.md"
EVIDENCE = ROOT / "docs/evidence/operational-envelope/46c51c92"
EXPECTED_COMMIT = "46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da"


def test_operational_envelope_is_complete_and_links_exist():
    text = DOC.read_text()
    for heading in (
        "## Is localqueue a fit for this workload?",
        "## How to read claims",
        "## Supported deployment model",
        "## Delivery, leases, and deduplication",
        "## Durability: NORMAL and FULL",
        "## EventBus operations",
        "## Performance and practical concurrency",
        "## Backup, restore, and corruption response",
        "## Evidence map",
    ):
        assert heading in text
    assert not re.search(r"\b(TODO|TBD|FIXME)\b", text)
    for target in re.findall(r"\]\(([^)#]+)(?:#[^)]+)?\)", text):
        assert (DOC.parent / target).exists(), target
    assert re.search(r"\| Exactly-once processing \| Unsupported \|", text)
    assert re.search(r"\| NFS \| (Unsupported|Untested) \|", text)
    assert re.search(r"\| SMB/CIFS \| (Unsupported|Untested) \|", text)


def test_versioned_reports_are_valid_and_pinned():
    for name in ("benchmark-standard.json", "benchmark-multiprocess.json"):
        report = json.loads((EVIDENCE / name).read_text())
        assert report["subject"]["commit_sha"] == EXPECTED_COMMIT
    evidence_map = DOC.read_text().split("## Evidence map", 1)[1]
    for claim in ("ARM64", "physical power loss", "network filesystems"):
        assert claim in evidence_map
