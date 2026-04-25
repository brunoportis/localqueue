from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..paths import default_queue_store_path, default_retry_store_path
from ..services.queue_worker import print_json as _print_json

if TYPE_CHECKING:
    from ..stores import QueueMessage

CONFIG_FILENAME = "config.yaml"


def parse_json(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"value must be valid JSON unless --raw is passed: {exc}"
        ) from exc


def read_value(value: str | None) -> str:
    if value is not None:
        return value
    if sys.stdin.isatty():
        raise ValueError("missing value; pass an argument or pipe data on stdin")
    stdin_value = sys.stdin.read()
    if stdin_value == "":
        raise ValueError("stdin did not contain a value")
    return stdin_value


def config_path() -> Path:
    config_home = os.environ.get("XDG_CONFIG_HOME")
    if config_home:
        return Path(config_home) / "localqueue" / CONFIG_FILENAME
    return Path.home() / ".config" / "localqueue" / CONFIG_FILENAME


def load_config(yaml: Any, path: Path | None = None) -> dict[str, Any]:
    resolved_config_path = path if path is not None else config_path()
    if not resolved_config_path.exists():
        return {}

    with resolved_config_path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file)

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(f"config must be a YAML mapping: {resolved_config_path}")
    return dict(loaded)


def write_config(
    yaml: Any,
    config: dict[str, Any],
    path: Path | None = None,
) -> None:
    resolved_config_path = path if path is not None else config_path()
    resolved_config_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_config_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(config, file, sort_keys=False)


def resolve_store_path(explicit: str | None, config: dict[str, Any]) -> str:
    return str(explicit or config.get("store_path") or default_queue_store_path())


def resolve_retry_store_path(explicit: str | None, config: dict[str, Any]) -> str:
    value = explicit if explicit is not None else config.get("retry_store_path")
    if value is None:
        return str(default_retry_store_path())
    return str(value)


def resolve_dead_letter_ttl(
    explicit: float | None, config: dict[str, Any]
) -> float | None:
    value = explicit if explicit is not None else config.get("dead_letter_ttl_seconds")
    if value is None:
        return None
    return float(value)


def resolve_retry_record_ttl(
    explicit: float | None, config: dict[str, Any]
) -> float | None:
    value = explicit if explicit is not None else config.get("retry_record_ttl_seconds")
    if value is None:
        return None
    return float(value)


def retention_settings(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "dead_letter_ttl_seconds": config.get("dead_letter_ttl_seconds"),
        "retry_record_ttl_seconds": config.get("retry_record_ttl_seconds"),
    }


def coerce_config_value(key: str, value: str) -> str | float:
    if key in {"dead_letter_ttl_seconds", "retry_record_ttl_seconds"}:
        ttl = float(value)
        if ttl < 0:
            raise ValueError(f"{key} must be greater than or equal to zero")
        return ttl
    return value


def filter_dead_letters(
    messages: list[QueueMessage],
    *,
    min_attempts: int | None,
    max_attempts: int | None,
    error_contains: str | None,
    failed_within: float | None,
) -> list[QueueMessage]:
    now = time.time()
    error_filter = error_contains.lower() if error_contains is not None else None
    cutoff = None if failed_within is None else now - failed_within
    return [
        message
        for message in messages
        if matches_dead_letter_filters(
            message,
            min_attempts=min_attempts,
            max_attempts=max_attempts,
            cutoff=cutoff,
            error_filter=error_filter,
        )
    ]


def matches_dead_letter_filters(
    message: QueueMessage,
    *,
    min_attempts: int | None,
    max_attempts: int | None,
    cutoff: float | None,
    error_filter: str | None,
) -> bool:
    if min_attempts is not None and message.attempts < min_attempts:
        return False
    if max_attempts is not None and message.attempts > max_attempts:
        return False
    if cutoff is not None:
        failed_at = message.failed_at
        if failed_at is None or failed_at < cutoff:
            return False
    if error_filter is None:
        return True
    last_error = message.last_error or {}
    haystack = " ".join(
        str(part)
        for part in (
            last_error.get("type"),
            last_error.get("message"),
            last_error.get("command"),
            last_error.get("stderr"),
        )
        if part is not None
    ).lower()
    return error_filter in haystack


def dead_letter_summary(messages: list[QueueMessage]) -> dict[str, Any]:
    by_error_type = Counter[str]()
    by_attempts = Counter[str]()
    by_worker_id = Counter[str]()
    failed_ats: list[float] = []
    for message in messages:
        error_type = "None"
        if message.last_error is not None:
            error_type = str(message.last_error.get("type") or "None")
        by_error_type[error_type] += 1
        by_attempts[str(message.attempts)] += 1
        worker_id = last_attempt_worker_id(message)
        if worker_id is not None:
            by_worker_id[worker_id] += 1
        if message.failed_at is not None:
            failed_ats.append(message.failed_at)
    summary: dict[str, Any] = {
        "count": len(messages),
        "by_error_type": dict(sorted(by_error_type.items())),
        "by_attempts": dict(sorted(by_attempts.items(), key=lambda item: int(item[0]))),
        "by_worker_id": dict(sorted(by_worker_id.items())),
    }
    if failed_ats:
        summary["failed_at"] = {
            "oldest": min(failed_ats),
            "newest": max(failed_ats),
        }
    return summary


def worker_health_summary(
    last_seen_by_worker_id: dict[str, float], *, stale_after: float
) -> dict[str, Any]:
    now = time.time()
    workers_active: dict[str, float] = {}
    workers_stale: dict[str, float] = {}
    for worker_id, last_seen in sorted(last_seen_by_worker_id.items()):
        age = max(now - last_seen, 0.0)
        if age > stale_after:
            workers_stale[worker_id] = age
        else:
            workers_active[worker_id] = age
    return {
        "stale_after_seconds": stale_after,
        "active": {
            "count": len(workers_active),
            "by_worker_id": workers_active,
        },
        "stale": {
            "count": len(workers_stale),
            "by_worker_id": workers_stale,
        },
    }


def last_attempt_worker_id(message: QueueMessage) -> str | None:
    for event in reversed(message.attempt_history):
        if event.get("type") == "leased":
            worker_id = event.get("leased_by")
            return None if worker_id is None else str(worker_id)
    return None


def emit_event(console: Any, event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        **{key: value for key, value in fields.items() if value is not None},
    }
    _print_json(console, payload)
