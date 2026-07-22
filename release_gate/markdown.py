from __future__ import annotations

from collections.abc import Mapping, Sequence


def evidence_markdown(title: str, report: Mapping[str, object]) -> str:
    subject = report.get("subject", {})
    summary = report.get("summary", {})
    lines = [f"# {title}", ""]
    if isinstance(subject, Mapping):
        lines.extend(
            [
                f"- Candidate SHA: `{subject.get('candidate_sha', 'unavailable')}`",
                f"- Package version: `{subject.get('package_version', 'unavailable')}`",
                f"- Native version: `{subject.get('native_version', 'unavailable')}`",
                f"- Candidate ref: `{subject.get('candidate_ref', 'unavailable')}`",
            ]
        )
    lines.extend(
        [f"- Status: **{report.get('status', report.get('passed', 'unknown'))}**", ""]
    )
    if isinstance(summary, Mapping):
        lines.extend(["## Summary", ""])
        lines.extend(f"- {key}: `{summary[key]}`" for key in sorted(summary))
        lines.append("")
    scenarios = report.get("scenarios")
    if isinstance(scenarios, Sequence) and not isinstance(scenarios, (str, bytes)):
        lines.extend(["## Scenarios", "", "| Scenario | Status |", "| --- | --- |"])
        for item in scenarios:
            if isinstance(item, Mapping):
                name = item.get("scenario", item.get("name", "unknown"))
                status = item.get("status", item.get("passed", "unknown"))
                lines.append(f"| {name} | {status} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
