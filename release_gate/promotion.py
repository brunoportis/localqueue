from __future__ import annotations

from collections.abc import Mapping, Sequence


class PromotionError(ValueError):
    pass


ALLOWED_CLAIMS = {
    "production-grade transactional core": {"ci", "wheels", "crash"},
    "production-ready for documented single-host workloads": {
        "ci",
        "wheels",
        "storage-compatibility",
        "canonical-benchmark",
        "soak",
        "crash",
        "chaos",
    },
    "validated for documented single-host workloads": {
        "ci",
        "wheels",
    },
}


def validate_confirmation(value: str, version: str) -> None:
    if value != f"publish v{version}":
        raise PromotionError(f"confirmation must be exactly 'publish v{version}'")


def validate_public_claim(
    claim: str,
    jobs: Mapping[str, str],
    limitations: Sequence[str],
    policy: Mapping[str, object] | None = None,
) -> None:
    required = ALLOWED_CLAIMS.get(claim)
    policy_limits: Sequence[str] = ()
    if policy is not None:
        claims = policy.get("claims")
        entry = claims.get(claim) if isinstance(claims, Mapping) else None
        if not isinstance(entry, Mapping):
            raise PromotionError("unsupported public claim")
        required = {str(item) for item in entry.get("required_jobs", [])}
        policy_limits = [
            str(item).lower() for item in entry.get("required_limitations", [])
        ]
    if required is None:
        raise PromotionError("unsupported public claim")
    missing = sorted(name for name in required if jobs.get(name) != "passed")
    if missing:
        raise PromotionError(f"public claim requires complete evidence: {missing}")
    lower = " ".join(limitations).lower()
    if claim == "production-ready for documented single-host workloads":
        required_limits = policy_limits or (
            "nfs",
            "smb",
            "multi-host",
            "exactly-once",
            "physical arm64",
            "physical power loss",
        )
        absent = [item for item in required_limits if item not in lower]
        if absent:
            raise PromotionError(
                f"strong public claim is missing limitations: {absent}"
            )
    forbidden_guarantees = (
        "nfs supported",
        "smb supported",
        "exactly-once supported",
        "exactly-once guaranteed",
        "physical arm64 validated",
        "power-loss guaranteed",
    )
    if any(item in lower for item in forbidden_guarantees):
        raise PromotionError("known limitations contradict the selected public claim")


def plan_git_promotion(
    main_sha: str, parent_sha: str, tag_sha: str | None, candidate_sha: str
) -> str:
    if tag_sha is not None and tag_sha != candidate_sha:
        raise PromotionError("existing tag points to a different SHA")
    if main_sha == candidate_sha:
        if tag_sha == candidate_sha:
            return "complete"
        raise PromotionError("main is promoted but the final tag is missing")
    if main_sha != parent_sha:
        raise PromotionError("main advanced after evidence collection")
    return "push"


def reusable_release(release: Mapping[str, object] | None, version: str) -> str:
    if release is None:
        return "create-draft"
    if release.get("tag") != f"v{version}":
        raise PromotionError("GitHub Release tag differs")
    return "reuse-draft" if release.get("draft") is True else "already-published"


def compare_pypi_files(
    expected: Sequence[Mapping[str, str]], actual: Sequence[Mapping[str, str]] | None
) -> str:
    if actual is None:
        return "publish"
    normalized_expected = sorted(
        (item["filename"], item["sha256"]) for item in expected
    )
    normalized_actual = sorted((item["filename"], item["sha256"]) for item in actual)
    if normalized_expected != normalized_actual:
        raise PromotionError("PyPI files or hashes differ from approved evidence")
    return "already-published"


def pypi_files(metadata: Mapping[str, object]) -> list[dict[str, str]]:
    urls = metadata.get("urls")
    if not isinstance(urls, Sequence):
        raise PromotionError("PyPI metadata has no files")
    result: list[dict[str, str]] = []
    for item in urls:
        if not isinstance(item, Mapping) or not isinstance(
            item.get("digests"), Mapping
        ):
            raise PromotionError("PyPI file metadata is incomplete")
        digest = item["digests"].get("sha256")
        filename = item.get("filename")
        if not isinstance(filename, str) or not isinstance(digest, str):
            raise PromotionError("PyPI file filename/hash is missing")
        result.append({"filename": filename, "sha256": digest})
    return sorted(result, key=lambda entry: entry["filename"])
