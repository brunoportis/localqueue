from __future__ import annotations

import json
import os
import tempfile
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
        if isinstance(s.get("multiprocess"), dict):
            s = s["multiprocess"]
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
