from __future__ import annotations

import importlib
import signal
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, TYPE_CHECKING

from .cli_commands import (
    CommandRegistryContext,
    register_config_commands,
    register_queue_commands as _register_non_worker_queue_commands,
    register_retry_commands,
)
from .cli_support import (
    CONFIG_FILENAME,
    coerce_config_value as _coerce_config_value,
    config_path as _config_path,
    dead_letter_summary as _dead_letter_summary,
    emit_event as _emit_event,
    filter_dead_letters as _filter_dead_letters,
    last_attempt_worker_id as _last_attempt_worker_id,
    load_config as _load_config,
    matches_dead_letter_filters as _matches_dead_letter_filters,
    parse_json as _parse_json,
    read_value as _read_value,
    resolve_dead_letter_ttl as _resolve_dead_letter_ttl,
    resolve_retry_record_ttl as _resolve_retry_record_ttl,
    resolve_retry_store_path as _resolve_retry_store_path,
    resolve_store_path as _resolve_store_path,
    retention_settings as _retention_settings,
    worker_health_summary as _worker_health_summary,
    write_config as _write_config,
)
from .cli_worker_commands import (
    QueueWorkerCliOptions as _QueueWorkerCliOptions,
    argument as _argument,
    queue_worker_command_signature as _queue_worker_command_signature,
    register_queue_exec_command,
    register_queue_process_command,
)
from .paths import default_queue_store_path, default_retry_store_path
from .queue import PersistentQueue
from .services.queue_worker import (
    _CommandExecutionError,
    _CommandNotFoundError,
    QueueIterationContext as _QueueIterationContext,
    QueueWorkerOptions as _QueueWorkerOptions,
    ShutdownState as _ShutdownState,
    command_handler as _command_handler,
    error_payload as _error_payload,
    finish_failed_message as _finish_failed_message,
    format_command as _format_command,
    message_payload as _message_payload,
    poll_timeout as _poll_timeout,
    print_json as _print_json,
    process_message as _process_message,
    process_queue_iteration as _process_queue_iteration,
    process_queue_messages as _process_queue_messages,
    truncate_output as _truncate_output,
)
from .stores import QueueMessage

if TYPE_CHECKING:
    from types import FrameType
    from collections.abc import Callable

DEFAULT_STORE_PATH = str(default_queue_store_path())
DEFAULT_RETRY_STORE_PATH = str(default_retry_store_path())

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
    command_context = CommandRegistryContext(
        typer=typer,
        console=console,
        err_console=err_console,
        config=config,
        config_path=_config_path,
        write_config=_write_config,
        print_json=_print_json,
        resolve_store_path=_resolve_store_path,
        resolve_retry_store_path=_resolve_retry_store_path,
        resolve_dead_letter_ttl=_resolve_dead_letter_ttl,
        resolve_retry_record_ttl=_resolve_retry_record_ttl,
        retention_settings=_retention_settings,
        coerce_config_value=_coerce_config_value,
        queue_factory=_queue,
        read_value=_read_value,
        parse_json=_parse_json,
        emit_event=_emit_event,
        message_payload=_message_payload,
        complete_message=_complete_message,
        shutdown_state=_shutdown_state,
        print_queue_stats=_print_queue_stats,
        worker_health_summary=_worker_health_summary,
        dead_letter_summary=_dead_letter_summary,
        print_dead_letters=_print_dead_letters,
    )
    app.add_typer(queue_app, name="queue")
    app.add_typer(retry_app, name="retry")
    app.add_typer(config_app, name="config")
    register_config_commands(config_app, yaml=yaml, context=command_context)
    _register_queue_commands(queue_app, typer, console, err_console, config, command_context)
    register_retry_commands(retry_app, context=command_context)
    return app


def _register_queue_commands(
    queue_app: Any,
    typer: Any,
    console: Any,
    err_console: Any,
    config: dict[str, Any],
    command_context: CommandRegistryContext,
) -> None:
    _register_non_worker_queue_commands(queue_app, context=command_context)
    register_queue_process_command(
        queue_app,
        typer=typer,
        console=console,
        err_console=err_console,
        config=config,
        validate_worker_loop_options=_validate_worker_loop_options,
        load_callable=_load_callable,
        queue_factory=_queue,
        resolve_store_path=_resolve_store_path,
        resolve_retry_store_path=_resolve_retry_store_path,
        shutdown_state=_shutdown_state,
        process_queue_messages=_process_queue_messages,
    )
    register_queue_exec_command(
        queue_app,
        typer=typer,
        console=console,
        err_console=err_console,
        config=config,
        validate_worker_loop_options=_validate_worker_loop_options,
        command_handler=_command_handler,
        queue_factory=_queue,
        resolve_store_path=_resolve_store_path,
        resolve_retry_store_path=_resolve_retry_store_path,
        shutdown_state=_shutdown_state,
        process_queue_messages=_process_queue_messages,
    )


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
    stale_after: float | None,
    shutdown: _ShutdownState,
) -> None:
    while True:
        stats = queue.stats()
        payload = stats.as_dict()
        if stale_after is not None:
            payload["worker_health"] = _worker_health_summary(
                stats.last_seen_by_worker_id, stale_after=stale_after
            )
        _print_json(console, payload)
        if not watch:
            return
        if shutdown.requested:
            return
        time.sleep(interval)
        if shutdown.requested:
            return


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
