from __future__ import annotations

import importlib
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from types import FrameType
from typing import Any, Iterator

from tenacity import wait_none

from .failure import is_permanent_failure
from .queue import PersistentQueue
from .retry import (
    PersistentRetryExhausted,
    PersistentRetrying,
    SQLiteAttemptStore,
)
from .store import QueueMessage

DEFAULT_STORE_PATH = "localqueue_queue.sqlite3"
DEFAULT_RETRY_STORE_PATH = "localqueue_retries.sqlite3"
CONFIG_FILENAME = "config.yaml"


@dataclass(frozen=True, slots=True)
class _ProcessResult:
    processed: bool
    last_error: dict[str, Any] | None = None
    final_state: str | None = None


@dataclass(slots=True)
class _ShutdownState:
    requested: bool = False


class _CommandExecutionError(RuntimeError):
    command: list[str]
    exit_code: int
    stdout: str
    stderr: str

    def __init__(
        self,
        command: list[str],
        *,
        exit_code: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.command = command
        self.exit_code = exit_code
        self.stdout = _truncate_output(stdout.strip())
        self.stderr = _truncate_output(stderr.strip())
        stderr_message = _truncate_output(stderr.strip())
        detail = f": {stderr_message}" if stderr_message else ""
        super().__init__(
            f"command exited with status {exit_code}: {_format_command(command)}"
            + detail
        )


class _CommandNotFoundError(_CommandExecutionError):
    def __init__(self, command: list[str]) -> None:
        super().__init__(
            command,
            exit_code=127,
            stdout="",
            stderr="command not found",
        )
        self.args = (f"command not found: {_format_command(command)}",)


def main(args: list[str] | None = None) -> None:
    try:
        from rich.console import Console
        import typer
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "The localqueue CLI requires optional dependencies. "
            "Install with: pip install 'localqueue[cli]'"
        ) from exc

    app = _build_app(typer, yaml, Console(), Console(stderr=True))
    app(args=args)


def _build_app(typer: Any, yaml: Any, console: Any, err_console: Any) -> Any:
    config = _load_config(yaml)
    app = typer.Typer(no_args_is_help=True)
    queue_app = typer.Typer(no_args_is_help=True)
    retry_app = typer.Typer(no_args_is_help=True)
    config_app = typer.Typer(no_args_is_help=True)
    app.add_typer(queue_app, name="queue")
    app.add_typer(retry_app, name="retry")
    app.add_typer(config_app, name="config")

    @config_app.command("path")
    def config_path() -> None:
        console.print(str(_config_path()))

    @config_app.command("show")
    def config_show() -> None:
        _print_json(console, config)

    @config_app.command("init")
    def config_init(
        store_path: str = typer.Option(DEFAULT_STORE_PATH, "--store-path"),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        dead_letter_ttl_seconds: float | None = typer.Option(
            None, "--dead-letter-ttl-seconds", min=0.0
        ),
        retry_record_ttl_seconds: float | None = typer.Option(
            None, "--retry-record-ttl-seconds", min=0.0
        ),
        force: bool = typer.Option(False, "--force"),
    ) -> None:
        path = _config_path()
        if path.exists() and not force:
            err_console.print(
                f"[red]config already exists:[/red] {path}; pass --force to overwrite"
            )
            raise typer.Exit(1)

        new_config: dict[str, Any] = {"store_path": store_path}
        if retry_store_path is not None:
            new_config["retry_store_path"] = retry_store_path
        if dead_letter_ttl_seconds is not None:
            new_config["dead_letter_ttl_seconds"] = dead_letter_ttl_seconds
        if retry_record_ttl_seconds is not None:
            new_config["retry_record_ttl_seconds"] = retry_record_ttl_seconds
        _write_config(yaml, new_config)
        config.clear()
        config.update(new_config)
        console.print(f"[green]wrote config:[/green] {path}")

    @config_app.command("set")
    def config_set(
        key: str,
        value: str,
    ) -> None:
        if key not in {
            "store_path",
            "retry_store_path",
            "dead_letter_ttl_seconds",
            "retry_record_ttl_seconds",
        }:
            err_console.print(
                "[red]unsupported config key:[/red] "
                + (
                    f"{key}; use store_path, retry_store_path, "
                    "dead_letter_ttl_seconds, or retry_record_ttl_seconds"
                )
            )
            raise typer.Exit(1)

        updated = dict(config)
        updated[key] = _coerce_config_value(key, value)
        _write_config(yaml, updated)
        config.clear()
        config.update(updated)
        _print_json(console, config)

    @queue_app.command("add")
    def queue_add(
        queue: str,
        value: str | None = typer.Option(None, "--value"),
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
        dedupe_key: str | None = typer.Option(None, "--dedupe-key"),
        raw: bool = typer.Option(False, "--raw"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        try:
            payload_value = _read_value(value)
            payload = payload_value if raw else _parse_json(payload_value)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        message = _queue(queue, _resolve_store_path(store_path, config)).put(
            payload, delay=delay, dedupe_key=dedupe_key
        )
        if log_events:
            _emit_event(
                err_console,
                "queue.enqueue",
                queue=queue,
                message_id=message.id,
                state=message.state,
                available_at=message.available_at,
            )
        _print_json(console, _message_payload(message))

    @queue_app.command("pop")
    def queue_pop(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        lease_timeout: float = typer.Option(30.0, "--lease-timeout", min=0.001),
        worker_id: str | None = typer.Option(None, "--worker-id"),
        block: bool = typer.Option(False, "--block"),
        timeout: float | None = typer.Option(None, "--timeout", min=0.0),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        try:
            message = _queue(
                queue,
                _resolve_store_path(store_path, config),
                lease_timeout=lease_timeout,
            ).get_message(block=block, timeout=timeout, leased_by=worker_id)
        except Empty as exc:
            err_console.print("[yellow]queue is empty[/yellow]")
            raise typer.Exit(1) from exc
        if log_events:
            _emit_event(
                err_console,
                "queue.lease",
                queue=message.queue,
                message_id=message.id,
                leased_by=message.leased_by,
                attempts=message.attempts,
                leased_until=message.leased_until,
            )
        _print_json(console, _message_payload(message))

    @queue_app.command("inspect")
    def queue_inspect(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        message = _queue(queue, _resolve_store_path(store_path, config)).inspect(
            message_id
        )
        if message is None:
            err_console.print(f"[red]message not found:[/red] {message_id}")
            raise typer.Exit(1)
        _print_json(console, _message_payload(message))

    @queue_app.command("ack")
    def queue_ack(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        _complete_message(
            typer,
            console,
            err_console,
            queue,
            _resolve_store_path(store_path, config),
            message_id,
            "ack",
            log_events=log_events,
        )

    @queue_app.command("release")
    def queue_release(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        _complete_message(
            typer,
            console,
            err_console,
            queue,
            _resolve_store_path(store_path, config),
            message_id,
            "release",
            delay=delay,
            log_events=log_events,
        )

    @queue_app.command("dead-letter")
    def queue_dead_letter(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        _complete_message(
            typer,
            console,
            err_console,
            queue,
            _resolve_store_path(store_path, config),
            message_id,
            "dead-letter",
            log_events=log_events,
        )

    @queue_app.command("size")
    def queue_size(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
    ) -> None:
        console.print(_queue(queue, _resolve_store_path(store_path, config)).qsize())

    @queue_app.command("stats")
    def queue_stats(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        watch: bool = typer.Option(False, "--watch"),
        interval: float = typer.Option(1.0, "--interval", min=0.001),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        persistent_queue = _queue(queue, _resolve_store_path(store_path, config))
        with _shutdown_state() as shutdown:
            _print_queue_stats(
                persistent_queue,
                console=console,
                watch=watch,
                interval=interval,
                shutdown=shutdown,
            )

    @queue_app.command("health")
    def queue_health(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        persistent_queue = _queue(queue, _resolve_store_path(store_path, config))
        _print_json(
            console,
            {
                "queue": queue,
                "stats": persistent_queue.stats().as_dict(),
                "dead_letters": _dead_letter_summary(persistent_queue.dead_letters()),
                "retention": _retention_settings(config),
            },
        )

    @queue_app.command("dead")
    def queue_dead(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        limit: int | None = typer.Option(None, "--limit", min=0),
        watch: bool = typer.Option(False, "--watch"),
        interval: float = typer.Option(1.0, "--interval", min=0.001),
        json_output: bool = typer.Option(False, "--json"),
        summary: bool = typer.Option(False, "--summary"),
        min_attempts: int | None = typer.Option(None, "--min-attempts", min=0),
        max_attempts: int | None = typer.Option(None, "--max-attempts", min=0),
        error_contains: str | None = typer.Option(None, "--error-contains"),
        failed_within: float | None = typer.Option(None, "--failed-within", min=0.0),
        dry_run: bool = typer.Option(False, "--dry-run"),
        prune_older_than: float | None = typer.Option(
            None, "--prune-older-than", min=0.0
        ),
    ) -> None:
        persistent_queue = _queue(queue, _resolve_store_path(store_path, config))
        if prune_older_than is not None or dry_run:
            older_than = _resolve_dead_letter_ttl(prune_older_than, config)
            if older_than is None:
                err_console.print(
                    "[red]pass --prune-older-than or configure "
                    "dead_letter_ttl_seconds[/red]"
                )
                raise typer.Exit(1)
            if any(
                (
                    watch,
                    summary,
                    limit is not None,
                    min_attempts is not None,
                    max_attempts is not None,
                    error_contains is not None,
                    failed_within is not None,
                )
            ):
                err_console.print(
                    "[red]pass --prune-older-than on its own; "
                    + "it cannot be combined with list or watch options[/red]"
                )
                raise typer.Exit(1)
            deleted = (
                persistent_queue.count_dead_letters_older_than(older_than=older_than)
                if dry_run
                else persistent_queue.prune_dead_letters(older_than=older_than)
            )
            _print_json(
                console,
                {
                    "dry_run": dry_run,
                    "older_than": older_than,
                    "would_delete" if dry_run else "deleted": deleted,
                },
            )
            return
        with _shutdown_state() as shutdown:
            _print_dead_letters(
                persistent_queue,
                console=console,
                limit=limit,
                watch=watch,
                interval=interval,
                shutdown=shutdown,
                summary=summary,
                min_attempts=min_attempts,
                max_attempts=max_attempts,
                error_contains=error_contains,
                failed_within=failed_within,
            )

    @retry_app.command("prune")
    def retry_prune(
        older_than: float | None = typer.Option(None, "--older-than", min=0.0),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        dry_run: bool = typer.Option(False, "--dry-run"),
    ) -> None:
        store = SQLiteAttemptStore(_resolve_retry_store_path(retry_store_path, config))
        try:
            resolved_older_than = _resolve_retry_record_ttl(older_than, config)
            if resolved_older_than is None:
                err_console.print(
                    "[red]pass --older-than or configure retry_record_ttl_seconds[/red]"
                )
                raise typer.Exit(1)
            now = time.time()
            deleted = (
                store.count_exhausted_older_than(
                    older_than=resolved_older_than, now=now
                )
                if dry_run
                else store.prune_exhausted(older_than=resolved_older_than, now=now)
            )
        finally:
            store.close()
        _print_json(
            console,
            {
                "dry_run": dry_run,
                "older_than": resolved_older_than,
                "would_delete" if dry_run else "deleted": deleted,
            },
        )

    @queue_app.command("requeue-dead")
    def queue_requeue_dead(
        queue: str,
        message_id: str | None = typer.Argument(None),
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
        all_: bool = typer.Option(False, "--all"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        persistent_queue = _queue(queue, _resolve_store_path(store_path, config))
        if all_:
            if message_id is not None:
                err_console.print(
                    "[red]pass either a message id or --all, not both[/red]"
                )
                raise typer.Exit(1)
            messages = persistent_queue.dead_letters()
            requeued = 0
            for message in messages:
                if persistent_queue.requeue_dead(message, delay=delay):
                    requeued += 1
            if log_events:
                _emit_event(
                    err_console,
                    "queue.requeue",
                    queue=queue,
                    all=True,
                    requeued=requeued,
                    delay=delay,
                )
            _print_json(console, {"requeued": requeued})
            return
        if message_id is None:
            err_console.print(
                "[red]pass a message id or use --all to requeue dead letters[/red]"
            )
            raise typer.Exit(1)
        message = QueueMessage(id=message_id, value=None, queue=queue)
        if not persistent_queue.requeue_dead(message, delay=delay):
            err_console.print(f"[red]dead-letter message not found:[/red] {message_id}")
            raise typer.Exit(1)
        if log_events:
            _emit_event(
                err_console,
                "queue.requeue",
                queue=queue,
                message_id=message_id,
                delay=delay,
            )
        _print_json(console, {"id": message_id, "state": "requeued"})

    @queue_app.command("purge")
    def queue_purge(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        deleted = _queue(queue, _resolve_store_path(store_path, config)).purge()
        if log_events:
            _emit_event(err_console, "queue.purge", queue=queue, deleted=deleted)
        console.print(deleted)

    @queue_app.command("process")
    def queue_process(
        queue: str,
        handler: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        max_jobs: int = typer.Option(1, "--max-jobs", min=1),
        forever: bool = typer.Option(False, "--forever"),
        max_tries: int = typer.Option(3, "--max-tries", min=1),
        lease_timeout: float = typer.Option(30.0, "--lease-timeout", min=0.001),
        worker_id: str | None = typer.Option(None, "--worker-id"),
        block: bool = typer.Option(False, "--block"),
        timeout: float | None = typer.Option(None, "--timeout", min=0.0),
        idle_sleep: float = typer.Option(1.0, "--idle-sleep", min=0.001),
        release_delay: float = typer.Option(0.0, "--release-delay", min=0.0),
        dead_letter_on_exhaustion: bool = typer.Option(
            True,
            "--dead-letter-on-exhaustion/--release-on-exhaustion",
        ),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        try:
            _validate_worker_loop_options(
                max_jobs=max_jobs,
                forever=forever,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        try:
            worker = _load_callable(handler)
        except (AttributeError, ImportError, TypeError, ValueError) as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        persistent_queue = _queue(
            queue,
            _resolve_store_path(store_path, config),
            lease_timeout=lease_timeout,
        )
        with _shutdown_state() as shutdown:
            exit_code = _process_queue_messages(
                persistent_queue,
                worker,
                console=console,
                err_console=err_console,
                shutdown=shutdown,
                retry_store_path=_resolve_retry_store_path(retry_store_path, config),
                max_jobs=max_jobs,
                forever=forever,
                max_tries=max_tries,
                worker_id=worker_id,
                block=block,
                timeout=timeout,
                idle_sleep=idle_sleep,
                release_delay=release_delay,
                dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                log_events=log_events,
                mode="process",
            )
        if exit_code:
            raise typer.Exit(exit_code)

    @queue_app.command("exec")
    def queue_exec(
        queue: str,
        command: list[str] = typer.Argument(
            ...,
            help="Command to run. Use '--' before commands that contain options.",
        ),
        store_path: str | None = typer.Option(None, "--store-path"),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        max_jobs: int = typer.Option(1, "--max-jobs", min=1),
        forever: bool = typer.Option(False, "--forever"),
        max_tries: int = typer.Option(3, "--max-tries", min=1),
        lease_timeout: float = typer.Option(30.0, "--lease-timeout", min=0.001),
        worker_id: str | None = typer.Option(None, "--worker-id"),
        block: bool = typer.Option(False, "--block"),
        timeout: float | None = typer.Option(None, "--timeout", min=0.0),
        idle_sleep: float = typer.Option(1.0, "--idle-sleep", min=0.001),
        release_delay: float = typer.Option(0.0, "--release-delay", min=0.0),
        dead_letter_on_exhaustion: bool = typer.Option(
            True,
            "--dead-letter-on-exhaustion/--release-on-exhaustion",
        ),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        try:
            _validate_worker_loop_options(
                max_jobs=max_jobs,
                forever=forever,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        persistent_queue = _queue(
            queue,
            _resolve_store_path(store_path, config),
            lease_timeout=lease_timeout,
        )
        with _shutdown_state() as shutdown:
            exit_code = _process_queue_messages(
                persistent_queue,
                _command_handler(command),
                console=console,
                err_console=err_console,
                shutdown=shutdown,
                retry_store_path=_resolve_retry_store_path(retry_store_path, config),
                max_jobs=max_jobs,
                forever=forever,
                max_tries=max_tries,
                worker_id=worker_id,
                block=block,
                timeout=timeout,
                idle_sleep=idle_sleep,
                release_delay=release_delay,
                dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                log_events=log_events,
                mode="exec",
            )
        if exit_code:
            raise typer.Exit(exit_code)

    return app


def _queue(
    name: str, store_path: str, *, lease_timeout: float = 30.0
) -> PersistentQueue:
    return PersistentQueue(name, store_path=store_path, lease_timeout=lease_timeout)


def _complete_message(
    typer: Any,
    console: Any,
    err_console: Any,
    queue: str,
    store_path: str,
    message_id: str,
    action: str,
    *,
    delay: float = 0.0,
    log_events: bool = False,
) -> None:
    persistent_queue = _queue(queue, store_path)
    message = QueueMessage(id=message_id, value=None, queue=queue)
    if action == "ack":
        completed = persistent_queue.ack(message)
    elif action == "release":
        completed = persistent_queue.release(message, delay=delay)
    elif action == "dead-letter":
        completed = persistent_queue.dead_letter(message)
    else:
        raise ValueError(f"unsupported queue action: {action}")

    if not completed:
        err_console.print(f"[red]message not found:[/red] {message_id}")
        raise typer.Exit(1)
    if log_events:
        _emit_event(
            err_console,
            f"queue.{action.replace('-', '_')}",
            queue=queue,
            message_id=message_id,
            delay=delay if action == "release" else None,
        )
    _print_json(console, {"id": message_id, "state": action})


def _process_message(
    queue: PersistentQueue,
    message: QueueMessage,
    handler: Callable[[Any], Any],
    *,
    retry_store: Any | None = None,
    retry_store_path: str | None,
    max_tries: int,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    log_events: bool = False,
    mode: str = "process",
    err_console: Any | None = None,
) -> _ProcessResult:
    retry_kwargs: dict[str, Any] = {
        "key": message.id,
        "max_tries": max_tries,
        "wait": wait_none(),
    }
    owned_retry_store: SQLiteAttemptStore | None = None
    if retry_store is not None:
        retry_kwargs["store"] = retry_store
    elif retry_store_path is not None:
        owned_retry_store = SQLiteAttemptStore(retry_store_path)
        retry_kwargs["store"] = owned_retry_store

    retryer = PersistentRetrying(**retry_kwargs)
    try:
        try:
            _ = retryer(handler, message.value)
        except PersistentRetryExhausted as exc:
            last_error = _error_payload(exc)
            _ = _finish_failed_message(
                queue,
                message,
                release_delay=release_delay,
                dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                error=exc,
            )
            if log_events and err_console is not None:
                _emit_event(
                    err_console,
                    f"{mode}.dead_letter",
                    queue=message.queue,
                    message_id=message.id,
                    attempts=message.attempts,
                    leased_by=message.leased_by,
                    last_error=last_error,
                )
            return _ProcessResult(
                processed=False, last_error=last_error, final_state="dead-letter"
            )
        except Exception as exc:
            last_error = _error_payload(exc)
            record = retryer.get_record(message.id)
            exhausted = record is not None and record.exhausted
            if is_permanent_failure(exc) or exhausted:
                _ = _finish_failed_message(
                    queue,
                    message,
                    release_delay=release_delay,
                    dead_letter_on_exhaustion=True
                    if is_permanent_failure(exc)
                    else dead_letter_on_exhaustion,
                    error=exc,
                )
                final_state = "dead-letter"
            else:
                _ = queue.release(message, delay=release_delay, error=exc)
                final_state = "release"
            if log_events and err_console is not None:
                _emit_event(
                    err_console,
                    f"{mode}.{final_state.replace('-', '_')}",
                    queue=message.queue,
                    message_id=message.id,
                    attempts=message.attempts,
                    leased_by=message.leased_by,
                    last_error=last_error,
                )
            return _ProcessResult(
                processed=False, last_error=last_error, final_state=final_state
            )

        acked = queue.ack(message)
        if log_events and err_console is not None:
            _emit_event(
                err_console,
                f"{mode}.ack",
                queue=message.queue,
                message_id=message.id,
                attempts=message.attempts,
                leased_by=message.leased_by,
            )
        return _ProcessResult(processed=acked, final_state="acked")
    finally:
        if owned_retry_store is not None:
            owned_retry_store.close()


def _process_queue_messages(
    queue: PersistentQueue,
    handler: Callable[[Any], Any],
    *,
    console: Any,
    err_console: Any,
    shutdown: _ShutdownState,
    retry_store_path: str | None,
    max_jobs: int,
    forever: bool,
    max_tries: int,
    worker_id: str | None,
    block: bool,
    timeout: float | None,
    idle_sleep: float,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    log_events: bool = False,
    mode: str = "process",
) -> int:
    processed = 0

    while forever or processed < max_jobs:
        if shutdown.requested:
            if forever:
                _print_json(console, {"state": "stopped", "processed": processed})
            return 0

        try:
            message = queue.get_message(
                block=block,
                timeout=_poll_timeout(forever, block, timeout),
                leased_by=worker_id,
            )
        except Empty:
            if forever:
                if not block:
                    time.sleep(idle_sleep)
                continue
            if processed == 0:
                if err_console is not None:
                    err_console.print("[yellow]queue is empty[/yellow]")
                return 1
            break

        if log_events and err_console is not None:
            _emit_event(
                err_console,
                f"{mode}.lease",
                queue=message.queue,
                message_id=message.id,
                leased_by=message.leased_by,
                attempts=message.attempts,
                leased_until=message.leased_until,
            )

        result = _process_message(
            queue,
            message,
            handler,
            retry_store_path=retry_store_path,
            max_tries=max_tries,
            release_delay=release_delay,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
            log_events=log_events,
            mode=mode,
            err_console=err_console,
        )
        if result.processed:
            processed += 1
            _print_json(console, {"id": message.id, "state": "acked"})
            continue

        payload = (
            _message_payload(current_message)
            if (current_message := queue.inspect(message.id)) is not None
            else {"id": message.id, "queue": message.queue}
        )
        _print_json(
            console,
            {
                **payload,
                "state": "failed",
                "last_error": result.last_error,
            },
        )
        if forever:
            continue
        return 1

    return 0


def _print_dead_letters(
    queue: PersistentQueue,
    *,
    console: Any,
    limit: int | None,
    watch: bool,
    interval: float,
    shutdown: _ShutdownState,
    summary: bool,
    min_attempts: int | None,
    max_attempts: int | None,
    error_contains: str | None,
    failed_within: float | None,
) -> None:
    while True:
        messages = _filter_dead_letters(
            queue.dead_letters(limit=limit),
            min_attempts=min_attempts,
            max_attempts=max_attempts,
            error_contains=error_contains,
            failed_within=failed_within,
        )
        if summary:
            _print_json(console, _dead_letter_summary(messages))
        else:
            _print_json(console, [_message_payload(message) for message in messages])
        if not watch:
            return
        if shutdown.requested:
            return
        time.sleep(interval)
        if shutdown.requested:
            return


def _print_queue_stats(
    queue: PersistentQueue,
    *,
    console: Any,
    watch: bool,
    interval: float,
    shutdown: _ShutdownState,
) -> None:
    while True:
        _print_json(console, queue.stats().as_dict())
        if not watch:
            return
        if shutdown.requested:
            return
        time.sleep(interval)
        if shutdown.requested:
            return


def _poll_timeout(forever: bool, block: bool, timeout: float | None) -> float | None:
    if forever and block and timeout is None:
        return 0.5
    return timeout


def _validate_worker_loop_options(*, max_jobs: int, forever: bool) -> None:
    if forever and max_jobs != 1:
        raise ValueError("pass either --forever or --max-jobs, not both")


@contextmanager
def _shutdown_state() -> Iterator[_ShutdownState]:
    state = _ShutdownState()

    def request_shutdown(_signum: int, _frame: FrameType | None) -> None:
        state.requested = True

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    try:
        yield state
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)


def _finish_failed_message(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    error: BaseException | str | None,
) -> bool:
    if dead_letter_on_exhaustion:
        return queue.dead_letter(message, error=error)
    return queue.release(message, delay=release_delay, error=error)


def _error_payload(error: BaseException | str | None) -> dict[str, Any] | None:
    if error is None:
        return None
    if isinstance(error, _CommandExecutionError):
        return {
            "type": type(error).__name__,
            "module": type(error).__module__,
            "message": str(error),
            "command": error.command,
            "exit_code": error.exit_code,
            "stdout": _truncate_output(error.stdout.strip()),
            "stderr": _truncate_output(error.stderr.strip()),
        }
    if isinstance(error, BaseException):
        error_type = type(error)
        return {
            "type": error_type.__name__,
            "module": error_type.__module__,
            "message": str(error),
        }
    return {"type": None, "module": None, "message": str(error)}


def _parse_json(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"value must be valid JSON unless --raw is passed: {exc}"
        ) from exc


def _read_value(value: str | None) -> str:
    if value is not None:
        return value
    if sys.stdin.isatty():
        raise ValueError("missing value; pass an argument or pipe data on stdin")
    stdin_value = sys.stdin.read()
    if stdin_value == "":
        raise ValueError("stdin did not contain a value")
    return stdin_value


def _config_path() -> Path:
    config_home = os.environ.get("XDG_CONFIG_HOME")
    if config_home:
        return Path(config_home) / "localqueue" / CONFIG_FILENAME
    return Path.home() / ".config" / "localqueue" / CONFIG_FILENAME


def _load_config(yaml: Any, path: Path | None = None) -> dict[str, Any]:
    config_path = path if path is not None else _config_path()
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file)

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(f"config must be a YAML mapping: {config_path}")
    return dict(loaded)


def _write_config(yaml: Any, config: dict[str, Any], path: Path | None = None) -> None:
    config_path = path if path is not None else _config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(config, file, sort_keys=False)


def _resolve_store_path(explicit: str | None, config: dict[str, Any]) -> str:
    return str(explicit or config.get("store_path") or DEFAULT_STORE_PATH)


def _resolve_retry_store_path(explicit: str | None, config: dict[str, Any]) -> str:
    value = explicit if explicit is not None else config.get("retry_store_path")
    if value is None:
        return DEFAULT_RETRY_STORE_PATH
    return str(value)


def _resolve_dead_letter_ttl(
    explicit: float | None, config: dict[str, Any]
) -> float | None:
    value = explicit if explicit is not None else config.get("dead_letter_ttl_seconds")
    if value is None:
        return None
    return float(value)


def _resolve_retry_record_ttl(
    explicit: float | None, config: dict[str, Any]
) -> float | None:
    value = explicit if explicit is not None else config.get("retry_record_ttl_seconds")
    if value is None:
        return None
    return float(value)


def _retention_settings(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "dead_letter_ttl_seconds": config.get("dead_letter_ttl_seconds"),
        "retry_record_ttl_seconds": config.get("retry_record_ttl_seconds"),
    }


def _coerce_config_value(key: str, value: str) -> str | float:
    if key in {"dead_letter_ttl_seconds", "retry_record_ttl_seconds"}:
        return float(value)
    return value


def _load_callable(spec: str) -> Callable[[Any], Any]:
    module_name, separator, attribute = spec.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("handler must use 'module:function' format")
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    importlib.invalidate_caches()
    module = importlib.import_module(module_name)
    target: Any = module
    for part in attribute.split("."):
        target = getattr(target, part)
    if not callable(target):
        raise TypeError(f"handler is not callable: {spec}")
    return target


def _command_handler(command: list[str]) -> Callable[[Any], None]:
    if not command:
        raise ValueError("command cannot be empty")

    def run_command(value: Any) -> None:
        try:
            completed = subprocess.run(
                command,
                input=_json_dumps(value) + "\n",
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise _CommandNotFoundError(command) from exc
        if completed.returncode != 0:
            raise _CommandExecutionError(
                command,
                exit_code=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )

    return run_command


def _message_payload(message: QueueMessage) -> dict[str, Any]:
    return {
        "id": message.id,
        "queue": message.queue,
        "state": message.state,
        "value": message.value,
        "attempts": message.attempts,
        "created_at": message.created_at,
        "available_at": message.available_at,
        "leased_until": message.leased_until,
        "leased_by": message.leased_by,
        "last_error": message.last_error,
        "failed_at": message.failed_at,
        "dedupe_key": message.dedupe_key,
        "attempt_history": message.attempt_history,
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _filter_dead_letters(
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
    filtered: list[QueueMessage] = []
    for message in messages:
        if min_attempts is not None and message.attempts < min_attempts:
            continue
        if max_attempts is not None and message.attempts > max_attempts:
            continue
        if cutoff is not None:
            failed_at = message.failed_at
            if failed_at is None or failed_at < cutoff:
                continue
        if error_filter is not None:
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
            if error_filter not in haystack:
                continue
        filtered.append(message)
    return filtered


def _dead_letter_summary(messages: list[QueueMessage]) -> dict[str, Any]:
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
        worker_id = _last_attempt_worker_id(message)
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


def _last_attempt_worker_id(message: QueueMessage) -> str | None:
    for event in reversed(message.attempt_history):
        if event.get("type") == "leased":
            worker_id = event.get("leased_by")
            return None if worker_id is None else str(worker_id)
    return None


def _format_command(command: list[str]) -> str:
    return " ".join(command)


def _truncate_output(value: str, *, limit: int = 500) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _print_json(console: Any, value: Any) -> None:
    console.print_json(_json_dumps(value))


def _emit_event(console: Any, event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        **{key: value for key, value in fields.items() if value is not None},
    }
    _print_json(console, payload)
