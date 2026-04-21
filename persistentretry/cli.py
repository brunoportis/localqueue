from __future__ import annotations

import importlib
import json
import os
import signal
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from types import FrameType
from typing import Any, Iterator

from tenacity import wait_none

from persistentqueue import PersistentQueue, QueueMessage
from persistentretry import (
    PersistentRetryExhausted,
    PersistentRetrying,
    SQLiteAttemptStore,
)

DEFAULT_STORE_PATH = "persistence_queue.sqlite3"
DEFAULT_RETRY_STORE_PATH = "persistence_db.sqlite3"
CONFIG_FILENAME = "config.yaml"


@dataclass(frozen=True, slots=True)
class _ProcessResult:
    processed: bool
    last_error: dict[str, Any] | None = None


@dataclass(slots=True)
class _ShutdownState:
    requested: bool = False


def main(args: list[str] | None = None) -> None:
    try:
        from rich.console import Console
        import typer
        import yaml
    except ImportError as exc:
        raise SystemExit(
            "The persistentretry CLI requires optional dependencies. "
            "Install with: pip install 'persistentretry[cli]'"
        ) from exc

    app = _build_app(typer, yaml, Console(), Console(stderr=True))
    app(args=args)


def _build_app(typer: Any, yaml: Any, console: Any, err_console: Any) -> Any:
    config = _load_config(yaml)
    app = typer.Typer(no_args_is_help=True)
    queue_app = typer.Typer(no_args_is_help=True)
    config_app = typer.Typer(no_args_is_help=True)
    app.add_typer(queue_app, name="queue")
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
        _write_config(yaml, new_config)
        config.clear()
        config.update(new_config)
        console.print(f"[green]wrote config:[/green] {path}")

    @config_app.command("set")
    def config_set(
        key: str,
        value: str,
    ) -> None:
        if key not in {"store_path", "retry_store_path"}:
            err_console.print(
                "[red]unsupported config key:[/red] "
                + f"{key}; use store_path or retry_store_path"
            )
            raise typer.Exit(1)

        updated = dict(config)
        updated[key] = value
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
        raw: bool = typer.Option(False, "--raw"),
    ) -> None:
        try:
            payload_value = _read_value(value)
            payload = payload_value if raw else _parse_json(payload_value)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        message = _queue(queue, _resolve_store_path(store_path, config)).put(
            payload, delay=delay
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
        _print_json(console, _message_payload(message))

    @queue_app.command("inspect")
    def queue_inspect(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
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
    ) -> None:
        _complete_message(
            typer,
            console,
            err_console,
            queue,
            _resolve_store_path(store_path, config),
            message_id,
            "ack",
        )

    @queue_app.command("release")
    def queue_release(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
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
        )

    @queue_app.command("dead-letter")
    def queue_dead_letter(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
    ) -> None:
        _complete_message(
            typer,
            console,
            err_console,
            queue,
            _resolve_store_path(store_path, config),
            message_id,
            "dead-letter",
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
    ) -> None:
        stats = _queue(queue, _resolve_store_path(store_path, config)).stats()
        _print_json(console, stats.as_dict())

    @queue_app.command("dead")
    def queue_dead(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        limit: int | None = typer.Option(None, "--limit", min=0),
    ) -> None:
        messages = _queue(queue, _resolve_store_path(store_path, config)).dead_letters(
            limit=limit
        )
        _print_json(console, [_message_payload(message) for message in messages])

    @queue_app.command("requeue-dead")
    def queue_requeue_dead(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
    ) -> None:
        persistent_queue = _queue(queue, _resolve_store_path(store_path, config))
        message = QueueMessage(id=message_id, value=None, queue=queue)
        if not persistent_queue.requeue_dead(message, delay=delay):
            err_console.print(f"[red]dead-letter message not found:[/red] {message_id}")
            raise typer.Exit(1)
        _print_json(console, {"id": message_id, "state": "requeued"})

    @queue_app.command("purge")
    def queue_purge(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
    ) -> None:
        console.print(_queue(queue, _resolve_store_path(store_path, config)).purge())

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
    ) -> None:
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
            return _ProcessResult(processed=False, last_error=last_error)
        except Exception as exc:
            last_error = _error_payload(exc)
            record = retryer.get_record(message.id)
            exhausted = record is not None and record.exhausted
            if exhausted:
                _ = _finish_failed_message(
                    queue,
                    message,
                    release_delay=release_delay,
                    dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                    error=exc,
                )
            else:
                _ = queue.release(message, delay=release_delay, error=exc)
            return _ProcessResult(processed=False, last_error=last_error)

        return _ProcessResult(processed=queue.ack(message))
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
                err_console.print("[yellow]queue is empty[/yellow]")
                return 1
            break

        result = _process_message(
            queue,
            message,
            handler,
            retry_store_path=retry_store_path,
            max_tries=max_tries,
            release_delay=release_delay,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        )
        if result.processed:
            processed += 1
            _print_json(console, {"id": message.id, "state": "acked"})
            continue

        _print_json(
            console,
            {"id": message.id, "state": "failed", "last_error": result.last_error},
        )
        if forever:
            continue
        return 1

    return 0


def _poll_timeout(forever: bool, block: bool, timeout: float | None) -> float | None:
    if forever and block and timeout is None:
        return 0.5
    return timeout


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
        return Path(config_home) / "persistentretry" / CONFIG_FILENAME
    return Path.home() / ".config" / "persistentretry" / CONFIG_FILENAME


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
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _print_json(console: Any, value: Any) -> None:
    console.print_json(_json_dumps(value))
