from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _escape(value: Any) -> str:
    if value is None:
        return "n/a"
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("`", "\\`")
        .replace("\r", " ")
        .replace("\n", "<br>")
    )


def _metric(series: dict[str, Any], percentile: str) -> Any:
    return series.get("summary", {}).get(percentile, "n/a")


def render_markdown(report: dict[str, Any]) -> str:
    profile = report.get("profile", {})
    subject = report.get("subject", {})
    environment = report.get("environment", {})
    overrides = profile.get("overrides", {})
    lines = [
        "# localqueue benchmark",
        "",
        f"- Package: `{_escape(subject.get('package_version'))}`",
        f"- Commit: `{_escape(subject.get('commit_sha'))}`",
        f"- Profile: `{_escape(profile.get('name', 'unknown'))}`",
        f"- Canonical: `{_escape(profile.get('canonical', True))}`",
        f"- Overrides: `{_escape(overrides or 'none')}`",
        "",
        "## Environment",
        "",
        "| Field | Value |",
        "|---|---|",
    ]
    for key in sorted(environment):
        lines.append(f"| {_escape(key)} | {_escape(environment[key])} |")
    lines.extend(
        [
            "",
            "## Scenarios",
            "",
            "| Scenario | Durability | P/C | Payload requested/actual | Messages | Produced/s | ACK/s | Claim p50/p95/p99 ns | Roundtrip p50/p95/p99 ns | Status |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    details: list[str] = []
    for original in report.get("scenarios", []):
        s = (
            original.get("multiprocess")
            if isinstance(original.get("multiprocess"), dict)
            else original
        )
        p = s.get("parameters", {})
        t = s.get("throughput", {})
        m = s.get("metric_series", {})
        claim = m.get("claim_latency", {})
        roundtrip = m.get("roundtrip_latency", {})
        single_summary = s.get("summary") or {}
        claim_cell = (
            "/".join(
                _escape(_metric(claim, key)) for key in ("p50_ns", "p95_ns", "p99_ns")
            )
            if claim
            else "/".join(
                _escape(single_summary.get(key, "n/a"))
                for key in ("p50_ns", "p95_ns", "p99_ns")
            )
        )
        roundtrip_cell = (
            "/".join(
                _escape(_metric(roundtrip, key))
                for key in ("p50_ns", "p95_ns", "p99_ns")
            )
            if roundtrip
            else "n/a"
        )
        lines.append(
            f"| {_escape(s.get('scenario_id', ''))} | {_escape(p.get('durability'))} | {_escape(p.get('producers', '-'))}/{_escape(p.get('consumers', '-'))} | {_escape(p.get('payload_requested_bytes'))}/{_escape(p.get('payload_serialized_bytes'))} | {_escape(p.get('messages', s.get('work_units', {}).get('messages')))} | {_escape(t.get('produced_per_second', single_summary.get('messages_per_second')))} | {_escape(t.get('acked_per_second'))} | {claim_cell} | {roundtrip_cell} | {_escape(s.get('status', 'failed'))} |"
        )
        scenario_id = _escape(s.get("scenario_id", "scenario"))
        details.extend(
            [
                "",
                f"### {scenario_id}",
                "",
                f"Serializer: `{_escape(p.get('serializer'))}`; padding: `{_escape(p.get('padding_method'))}`.",
            ]
        )
        processes = s.get("processes", [])
        if processes:
            details.extend(
                [
                    "",
                    "| Process | Role | Status | Exit code | Peak RSS bytes | RSS method |",
                    "|---|---|---|---:|---:|---|",
                ]
            )
            for process in sorted(processes, key=lambda item: str(item.get("id", ""))):
                details.append(
                    f"| {_escape(process.get('id'))} | {_escape(process.get('role'))} | {_escape(process.get('status'))} | {_escape(process.get('exit_code'))} | {_escape(process.get('peak_rss_bytes'))} | {_escape(process.get('rss_method'))} |"
                )
        sqlite = s.get("sqlite", original.get("sqlite", {}))
        if sqlite:
            details.extend(
                [
                    "",
                    "SQLite: "
                    + ", ".join(
                        f"`{_escape(key)}={_escape(sqlite[key])}`"
                        for key in sorted(sqlite)
                    )
                    + ".",
                ]
            )
        files = s.get("files", {})
        if files:
            details.extend(
                ["", "| Phase | File | Exists | Size bytes |", "|---|---|---|---:|"]
            )
            for phase in sorted(files):
                snapshot = files[phase]
                if isinstance(snapshot, dict) and "exists" in snapshot:
                    snapshot = {phase: snapshot}
                for file_name in sorted(snapshot):
                    item = snapshot[file_name]
                    details.append(
                        f"| {_escape(phase)} | {_escape(file_name)} | {_escape(item.get('exists'))} | {_escape(item.get('size_bytes'))} |"
                    )
        correctness = s.get("correctness", original.get("correctness", {}))
        details.extend(
            [
                "",
                f"Correctness: `{_escape(correctness.get('ok'))}`; ID validation: `{_escape(correctness.get('id_validation'))}`; stats: `{_escape(correctness.get('stats', correctness.get('stats_after')))}`; integrity: `{_escape(correctness.get('integrity'))}`.",
            ]
        )
        if s.get("error") or original.get("error"):
            details.append(
                f"Failure: `{_escape(s.get('error') or original.get('error'))}`."
            )
    lines.extend(details)
    lines.extend(
        [
            "",
            "## Limitations",
            "",
            "Monotonic timestamps are comparable only between processes on the same host. Percentiles describe deterministic samples, while throughput uses total completed messages. Scheduler, CPU frequency, cache, filesystem, temperature, and virtualization affect results; no performance threshold is applied.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_file(source: Path, output: Path) -> None:
    data = json.loads(source.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1:
        raise ValueError("unsupported benchmark schema_version")
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(render_markdown(data))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
