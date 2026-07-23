"""Render public release notes from the versioned pre-GO candidate notes."""

from __future__ import annotations

import re


class ReleaseNotesError(ValueError):
    pass


def render_release_body(notes: str, claim: str, version: str) -> str:
    """Make the immutable public body for one approved version and claim."""
    heading = "## Proposed public wording"
    if heading not in notes:
        raise ReleaseNotesError(
            "candidate notes have no proposed public wording section"
        )
    body = notes.split(heading, maxsplit=1)[0].rstrip()
    tag_root = f"https://github.com/brunoportis/localqueue/blob/v{version}/"
    body = body.replace(
        "This candidate consolidates the changes since v1.1.2. The final public claim is\n"
        "deliberately left to the human promotion gate and must not exceed the collected\n"
        "evidence.",
        "This release consolidates the changes since v1.1.2. Its approved public claim\n"
        "is constrained by the attached release evidence.",
    )
    body = body.replace("The candidate does not change", "This release does not change")
    body = re.sub(
        r"The complete evidence manifest, distribution inventory, checksums, CI summary,\n"
        r"soak/crash/chaos reports, compatibility reports, benchmarks, documentation audit,\n"
        r"open-issue audit, and security audit will be attached to the GitHub Release after\n"
        r"successful human-approved promotion\.",
        "The complete evidence manifest, distribution inventory, checksums, CI summary,\n"
        "soak/crash/chaos reports, compatibility reports, benchmarks, documentation audit,\n"
        "open-issue audit, and security audit are attached to this GitHub Release.",
        body,
    )
    body = re.sub(
        r"\]\(\.\./docs/(operational-envelope|storage-compatibility)\.md\)",
        lambda match: f"]({tag_root}docs/{match.group(1)}.md)",
        body,
    )
    final = f"{body}\n\n## Approved public claim\n\n{claim}\n"
    forbidden = (
        "candidate",
        "will be attached",
        "proposed public wording",
    )
    if any(phrase in final.lower() for phrase in forbidden):
        raise ReleaseNotesError("public release body still contains pre-GO language")
    if re.search(r"after successful .*promotion", final, re.IGNORECASE):
        raise ReleaseNotesError(
            "public release body still contains future promotion language"
        )
    if re.search(r"\]\(\.\.?/", body):
        raise ReleaseNotesError("public release body still contains a relative link")
    if final.count("## Approved public claim") != 1:
        raise ReleaseNotesError(
            "public release body must contain exactly one approved claim"
        )
    return final


def validate_reusable_draft_body(existing_body: str, expected_body: str) -> None:
    if existing_body != expected_body:
        raise ReleaseNotesError(
            "existing draft release body differs from rendered body"
        )
