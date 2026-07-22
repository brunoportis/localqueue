from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

SCENARIO_NAMES = (
    "disk-full",
    "readonly",
    "lock-timeout",
    "wal-recovery",
    "corruption",
    "synchronous-normal",
    "synchronous-full",
    "producer-termination",
    "maintenance-termination",
    "backup-restore",
)
SMOKE_SCENARIOS = (
    "lock-timeout",
    "wal-recovery",
    "synchronous-normal",
    "backup-restore",
)

Counts = dict[str, int]


@dataclass
class ScenarioResult:
    name: str
    durability_mode: str | None = None
    status: str = "failed"
    expected_outcome: str = ""
    operation_confirmed_to_caller: bool | None = None
    retry_safe: bool | None = None
    retry_attempted: bool = False
    retry_succeeded: bool = False
    error: dict[str, Any] | None = None
    pragmas: dict[str, Any] = field(default_factory=dict)
    counts_before: Counts | None = None
    counts_after_failure: Counts | None = None
    counts_after_recovery: Counts | None = None
    integrity_check: str | None = None
    invariants: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    skip_reason: str | None = None
    required_invariants: frozenset[str] = field(default_factory=frozenset, repr=False)
    required_fields: frozenset[str] = field(default_factory=frozenset, repr=False)
    fresh_process_required: bool = field(default=False, repr=False)
    fresh_process_validated: bool = False

    def invariant(self, name: str, passed: bool, detail: str) -> None:
        self.invariants.append({"name": name, "passed": passed, "detail": detail})

    def finish(self) -> "ScenarioResult":
        if self.status == "passed":
            names = {item["name"] for item in self.invariants}
            complete = bool(self.invariants)
            complete = complete and self.required_invariants.issubset(names)
            complete = complete and all(item["passed"] for item in self.invariants)
            complete = complete and all(
                getattr(self, field_name) is not None
                for field_name in self.required_fields
            )
            if self.retry_safe is True:
                complete = complete and self.retry_attempted and self.retry_succeeded
            if self.fresh_process_required:
                complete = complete and self.fresh_process_validated
            self.status = "passed" if complete else "failed"
        return self


ScenarioFn = Callable[[str, Path], ScenarioResult]
