from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def render_markdown(report: dict[str, Any]) -> str:
    profile = report.get("profile", {})
    lines = [
        "# localqueue benchmark",
        "",
        f"- Profile: `{profile.get('name', 'unknown')}`",
        f"- Canonical: `{profile.get('canonical', True)}`",
        "",
        "| Scenario | P/C | Payload | Messages | ACK/s | Claim p95 (ns) | Roundtrip p95 (ns) | Correctness |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for s in report.get("scenarios", []):
        p = s.get("parameters", {})
        t = s.get("throughput", {})
        m = s.get("metric_series", {})
        c = m.get("claim_latency", {}).get("summary", {}).get("p95_ns", "n/a")
        r = m.get("roundtrip_latency", {}).get("summary", {}).get("p95_ns", "n/a")
        lines.append(
            f"| {s.get('scenario_id', '')} | {p.get('producers', '-')}/{p.get('consumers', '-')} | {p.get('payload_requested_bytes', '-')} | {p.get('messages', '-')} | {t.get('acked_per_second', 'n/a')} | {c} | {r} | {s.get('status', 'failed')} |"
        )
    return "\n".join(lines) + "\n"


def render_file(source: Path, output: Path) -> None:
    data = json.loads(source.read_text(encoding="utf-8"))
    output.write_text(render_markdown(data), encoding="utf-8")
