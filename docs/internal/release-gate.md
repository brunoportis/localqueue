# v1.2.0 release evidence and promotion gate

The v1.2.0 release uses three explicit phases. A candidate is a single immutable
commit on `release-candidate/v1.2.0`; its SHA, versions, distributions, reports,
release notes, and evidence manifest must remain identical from collection through
publication. Neither candidate preparation nor evidence collection updates `main`,
creates the final tag, creates a GitHub Release, or publishes to PyPI.

## A. Prepare the candidate

1. Open **Actions → Prepare release candidate → Run workflow** on `main`.
2. Select `dry-run` (the default). Confirm that the latest declared, tagged, and
   PyPI versions are all v1.1.2 and that the calculated next version is v1.2.0.
3. Run the workflow again with `prepare-candidate`.
4. Record the base main SHA, candidate SHA, candidate branch, release-notes path,
   changelog section, and evidence-run URL from the job summary.

Preparation runs Python Semantic Release locally, creates the version/changelog
commit, updates `uv.lock` with pinned `uv==0.11.6`, generates
`release-notes/v1.2.0.md`, and amends all of those files into that same commit.
The lock update is accepted only when the editable `localqueue` entry changes to
v1.2.0; any third-party resolution change fails. A final `uv lock --check` and
four-way Python/Cargo/native/uv version check are required before the workflow
removes Semantic Release's local tag and pushes only the candidate branch. The
branch must be exactly one commit above the base `main`. An existing branch at a
different SHA is a hard failure and is never force-pushed.

Do not create `release-candidate/v1.2.0` by hand. If the candidate needs a fix,
emit **NO-GO**, merge the fix into `main`, remove the obsolete candidate branch
through normal maintainer review, and run phase A again. Because `main` changed,
the old evidence cannot be reused.

## B. Collect and review evidence

Follow the linked **Release candidate evidence** run. It checks out the exact
candidate SHA and builds 25 wheels (five platforms by CPython 3.10–3.14) plus one
sdist. Every inventory entry records its own status: CPython 3.13 is smoke-tested
on Linux x86_64, macOS x86_64, macOS arm64, and Windows x86_64; the other built
wheels are `built-not-smoke-tested`; Linux ARM64 is
`artifact-validated-not-physical-smoke` and is never described as physical ARM64
smoke. Compressed platform tags are parsed with `packaging`, preserving every tag
emitted by Maturin, including manylinux_2_17 and manylinux2014.

The workflow executes complete CI, the scheduled 1,800-second/50,000-message soak,
all deterministic crash scenarios and controls, the complete `ci` chaos profile,
online and offline storage compatibility, standard and canonical multiprocess
benchmarks, documentation checks, an open-issue audit, and a security audit. Wheel
consumers use the exact Linux wheel produced by the distribution build; promotion
does not rebuild it.

Download `release-evidence-v1.2.0` from the run. Review
`release-evidence-summary.md`, then validate that:

- every report and distribution identifies the recorded candidate SHA/ref/version;
- `evidence-manifest.json` has `overall_status: passed` and `selected_claim: null`;
- every distribution checksum matches `wheel-inventory.json`;
- open P0/P1 correctness, corruption, silent-data-loss, security, concurrency,
  lease-fencing, or release-blocker issues are absent;
- #14 and #32 are classified as release-program metadata;
- #31 is a claim limitation, not a formal blocker;
- private vulnerability reporting is `enabled`, or the reviewer independently
  confirms it when the workflow token reports `manual_confirmation_required`;
- the release notes retain the NFS/SMB/multi-host/exactly-once, physical ARM64,
  and physical power-loss limitations.

Any missing, divergent, or stale item is **NO-GO**. Do not repair an evidence
bundle manually. Fix the tooling or source on `main`, prepare a new candidate, and
collect a new complete bundle against its new SHA.

## C. Human GO and publication

Only after review and explicit human **GO**, open **Actions → Wheels → Run
workflow** on `main` and provide:

- `candidate_run_id`: the successful phase-B run ID;
- `candidate_sha`, `candidate_version`, and `candidate_ref`: exactly as recorded;
- `public_claim`: one exact phrase from `release/claims-policy.json`;
- `private_vulnerability_reporting_confirmed`: true only after checking the
  repository setting when the evidence could not read it;
- `confirmation`: exactly `publish v1.2.0`.

The repository administrator must keep the `pypi` environment restricted to the
`main` deployment branch with required human reviewers. The PyPI Trusted Publisher
must remain bound to this repository, `.github/workflows/wheels.yml`, and the
`pypi` environment. The workflow itself fails unless both `github.ref` and
`github.workflow_ref` prove that its loaded definition came from `main`; checking
out `main` alone is not treated as sufficient.

The read-only `validate-candidate` job downloads the named evidence artifact from
that run, verifies every hash/report/source document, requires the candidate parent
to still equal `origin/main`, repeats the issue/security audits, checks the claim,
and proves the version/tag/Release/PyPI state is safe. Only then does the `pypi`
environment request approval.

After approval, `promote-and-publish` repeats mutable checks, pushes the approved
SHA to `main` and `refs/tags/v1.2.0` atomically on the initial run, creates or reuses
a draft GitHub Release, uploads the already-tested bytes, downloads and hashes the
assets again, and runs `uv publish --trusted-publishing always`. It verifies every
PyPI filename/hash before making the GitHub Release public and latest.

## Recovery after a partial failure

Rerun **Wheels** with exactly the same inputs. Never force-push, delete a published
release, replace a tag, or use `--clobber`.

- Candidate not promoted: the initial atomic main/tag push is attempted.
- Main and tag already point to the candidate: promotion resumes after Git.
- Only main or the correct tag exists: the missing ref may be created without force;
  any different SHA fails hard.
- Draft Release exists: its body must match. Existing assets are reused only when
  their downloaded SHA-256 equals the approved local asset; missing assets upload.
- PyPI failed or is absent: the exact approved distributions are published again.
- PyPI already has the version: every filename/hash must match before it is treated
  as complete; any difference fails hard.
- GitHub Release already public: it is never deleted or replaced. The run succeeds
  idempotently only when its body/assets and PyPI files are already exact.
- `main` advanced to another commit: stop. A new candidate and evidence run are
  required.

## Completion checklist

After the workflow succeeds, independently confirm v1.2.0 and all 26 files on PyPI,
confirm the public/latest GitHub Release and attached evidence, and confirm `main`
and `v1.2.0` resolve to the approved SHA. Only then close #32. Close the parent epic
only after its own definition of done is also satisfied.
