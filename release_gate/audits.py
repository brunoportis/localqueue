from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Iterable, Mapping

PRIORITY_RE = re.compile(r"\bPriority:\s*(P[0-3])\b", re.IGNORECASE)
RELEVANCE = {
    "correctness",
    "silent data loss",
    "data loss",
    "corruption",
    "concurrency",
    "lease fencing",
    "release blocker",
    "security",
}


class AuditError(ValueError):
    pass


def _labels(issue: Mapping[str, object]) -> list[str]:
    result: list[str] = []
    for label in issue.get("labels", []):  # type: ignore[union-attr]
        result.append(
            str(label.get("name", "")) if isinstance(label, Mapping) else str(label)
        )
    return result


def _priority(issue: Mapping[str, object]) -> str | None:
    labels = _labels(issue)
    for label in labels:
        if label.upper() in {"P0", "P1", "P2", "P3"}:
            return label.upper()
    match = PRIORITY_RE.search(str(issue.get("body", "")))
    return match.group(1).upper() if match else None


def audit_open_issues(
    issues: Iterable[Mapping[str, object]], exceptions: Iterable[Mapping[str, object]]
) -> dict[str, object]:
    valid_exceptions: dict[int, Mapping[str, object]] = {}
    for exception in exceptions:
        required = {"issue", "rationale", "evidence", "approver_requirement"}
        if required - exception.keys() or any(
            not exception.get(key) for key in required
        ):
            raise AuditError(
                "invalid exception: rationale, evidence and approver_requirement are required"
            )
        valid_exceptions[int(exception["issue"])] = exception
    reviewed: list[dict[str, object]] = []
    blockers: list[dict[str, object]] = []
    limitations: list[dict[str, object]] = []
    for issue in sorted(issues, key=lambda item: int(item["number"])):
        number = int(issue["number"])
        priority = _priority(issue)
        labels = _labels(issue)
        text = " ".join(
            (str(issue.get("title", "")), str(issue.get("body", "")), " ".join(labels))
        ).lower()
        categories = sorted(term for term in RELEVANCE if term in text)
        entry = {
            "issue": number,
            "title": str(issue.get("title", "")),
            "priority": priority,
            "labels": labels,
            "categories": categories,
        }
        reviewed.append(entry)
        if number in {14, 32}:
            entry["disposition"] = "release-program-meta"
        elif number == 31 and priority == "P2":
            entry["disposition"] = "known-limitation"
            limitations.append(entry)
        elif priority in {"P0", "P1"}:
            if number in valid_exceptions:
                entry["disposition"] = "versioned-exception"
                entry["exception"] = dict(valid_exceptions[number])
            else:
                entry["disposition"] = "blocker"
                blockers.append(entry)
        else:
            entry["disposition"] = "reviewed-non-blocking"
    if blockers:
        numbers = ", ".join(f"#{item['issue']}" for item in blockers)
        raise AuditError(f"open P0/P1 release blocker(s): {numbers}")
    return {
        "status": "passed",
        "reviewed": reviewed,
        "blockers": [],
        "limitations": limitations,
    }


def audit_security(
    policy_path: Path,
    *,
    private_reporting: bool | None,
    gitleaks: bool,
    cargo_deny: bool,
) -> dict[str, object]:
    if not policy_path.is_file():
        raise AuditError(".github/SECURITY.md is missing")
    content = policy_path.read_text(encoding="utf-8")
    links = re.findall(r"https://[^)\s]+", content)
    if not links or any(not link.startswith("https://github.com/") for link in links):
        raise AuditError("security policy must contain valid GitHub HTTPS links")
    if not gitleaks or not cargo_deny:
        raise AuditError("Gitleaks and cargo-deny must pass")
    private_status = (
        "enabled"
        if private_reporting is True
        else "disabled"
        if private_reporting is False
        else "manual_confirmation_required"
    )
    status = "passed" if private_status == "enabled" else private_status
    return {
        "status": status,
        "security_policy": str(policy_path),
        "links": sorted(links),
        "gitleaks": "passed",
        "cargo_deny": "passed",
        "private_vulnerability_reporting": private_status,
    }


def audit_release_dependencies(
    issues: Iterable[Mapping[str, object]],
    policy: Iterable[Mapping[str, object]],
    changelog: str,
    is_ancestor: Callable[[str], bool],
) -> dict[str, object]:
    by_number = {int(issue["number"]): issue for issue in issues}
    reviewed: list[dict[str, object]] = []
    failures: list[str] = []
    lowered_changelog = changelog.lower()
    for dependency in policy:
        number = int(dependency["issue"])
        issue = by_number.get(number)
        if issue is None or issue.get("state") != "CLOSED":
            failures.append(f"#{number} is not closed")
            continue
        references = issue.get("closedByPullRequestsReferences", {})
        nodes = references.get("nodes", []) if isinstance(references, Mapping) else []
        merged_commits = [
            str(node["mergeCommit"]["oid"])
            for node in nodes
            if isinstance(node, Mapping)
            and node.get("merged") is True
            and isinstance(node.get("mergeCommit"), Mapping)
            and node["mergeCommit"].get("oid")
        ]
        ancestors = [commit for commit in merged_commits if is_ancestor(commit)]
        if not ancestors:
            failures.append(
                f"#{number} has no merged closing PR in the candidate history"
            )
        terms = [str(term).lower() for term in dependency.get("changelog_terms", [])]
        missing_terms = [term for term in terms if term not in lowered_changelog]
        if missing_terms:
            failures.append(f"#{number} is missing changelog terms: {missing_terms}")
        reviewed.append(
            {
                "issue": number,
                "state": issue.get("state"),
                "merged_candidate_commits": sorted(ancestors),
                "changelog_terms": terms,
            }
        )
    if failures:
        raise AuditError("release dependency audit failed: " + "; ".join(failures))
    return {"status": "passed", "reviewed": reviewed}
