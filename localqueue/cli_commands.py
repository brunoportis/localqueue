from __future__ import annotations

from dataclasses import dataclass
from queue import Empty
from typing import TYPE_CHECKING, Any, Callable

from .retry import SQLiteAttemptStore
from .stores import QueueMessage

if TYPE_CHECKING:
    from .queue import PersistentQueue


@dataclass(frozen=True, slots=True)
class CommandRegistryContext:
    typer: Any
    console: Any
    err_console: Any
    config: dict[str, Any]
    config_path: Callable[..., Any]
    write_config: Callable[..., None]
    print_json: Callable[..., None]
    resolve_store_path: Callable[..., str]
    resolve_retry_store_path: Callable[..., str]
    resolve_dead_letter_ttl: Callable[..., float | None]
    resolve_retry_record_ttl: Callable[..., float | None]
    retention_settings: Callable[..., dict[str, Any]]
    coerce_config_value: Callable[[str, str], str | float]
    queue_factory: Callable[..., PersistentQueue]
    read_value: Callable[[str | None], str]
    parse_json: Callable[[str], Any]
    emit_event: Callable[..., None]
    message_payload: Callable[..., dict[str, Any]]
    complete_message: Callable[..., None]
    shutdown_state: Callable[..., Any]
    print_queue_stats: Callable[..., None]
    worker_health_summary: Callable[..., dict[str, Any]]
    dead_letter_summary: Callable[..., dict[str, Any]]
    print_dead_letters: Callable[..., None]


def register_config_commands(
    config_app: Any,
    *,
    yaml: Any,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

    @config_app.command("path")
    def config_path() -> None:
        console.print(str(context.config_path()))

    @config_app.command("show")
    def config_show() -> None:
        context.print_json(console, config)

    @config_app.command("init")
    def config_init(
        store_path: str | None = typer.Option(None, "--store-path"),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        dead_letter_ttl_seconds: float | None = typer.Option(
            None, "--dead-letter-ttl-seconds", min=0.0
        ),
        retry_record_ttl_seconds: float | None = typer.Option(
            None, "--retry-record-ttl-seconds", min=0.0
        ),
        force: bool = typer.Option(False, "--force"),
    ) -> None:
        path = context.config_path()
        if path.exists() and not force:
            err_console.print(
                f"[red]config already exists:[/red] {path}; pass --force to overwrite"
            )
            raise typer.Exit(1)

        new_config: dict[str, Any] = {
            "store_path": context.resolve_store_path(store_path, {})
        }
        if retry_store_path is not None:
            new_config["retry_store_path"] = retry_store_path
        if dead_letter_ttl_seconds is not None:
            new_config["dead_letter_ttl_seconds"] = dead_letter_ttl_seconds
        if retry_record_ttl_seconds is not None:
            new_config["retry_record_ttl_seconds"] = retry_record_ttl_seconds
        context.write_config(yaml, new_config)
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
        updated[key] = context.coerce_config_value(key, value)
        context.write_config(yaml, updated)
        config.clear()
        config.update(updated)
        context.print_json(console, config)


def register_queue_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    _register_queue_basic_commands(queue_app, context=context)
    _register_queue_maintenance_commands(queue_app, context=context)


def _register_queue_basic_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

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
            payload_value = context.read_value(value)
            payload = payload_value if raw else context.parse_json(payload_value)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        message = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        ).put(payload, delay=delay, dedupe_key=dedupe_key)
        if log_events:
            context.emit_event(
                err_console,
                "queue.enqueue",
                queue=queue,
                message_id=message.id,
                state=message.state,
                available_at=message.available_at,
            )
        context.print_json(console, context.message_payload(message))

    _register_queue_message_commands(queue_app, context=context)


def _register_queue_message_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

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
            message = context.queue_factory(
                queue,
                context.resolve_store_path(store_path, config),
                lease_timeout=lease_timeout,
            ).get_message(block=block, timeout=timeout, leased_by=worker_id)
        except Empty as exc:
            err_console.print("[yellow]queue is empty[/yellow]")
            raise typer.Exit(1) from exc
        if log_events:
            context.emit_event(
                err_console,
                "queue.lease",
                queue=message.queue,
                message_id=message.id,
                leased_by=message.leased_by,
                attempts=message.attempts,
                leased_until=message.leased_until,
            )
        context.print_json(console, context.message_payload(message))

    @queue_app.command("inspect")
    def queue_inspect(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        message = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        ).inspect(message_id)
        if message is None:
            err_console.print(f"[red]message not found:[/red] {message_id}")
            raise typer.Exit(1)
        context.print_json(console, context.message_payload(message))

    @queue_app.command("ack")
    def queue_ack(
        queue: str,
        message_id: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        context.complete_message(
            typer,
            console,
            err_console,
            queue,
            context.resolve_store_path(store_path, config),
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
        context.complete_message(
            typer,
            console,
            err_console,
            queue,
            context.resolve_store_path(store_path, config),
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
        context.complete_message(
            typer,
            console,
            err_console,
            queue,
            context.resolve_store_path(store_path, config),
            message_id,
            "dead-letter",
            log_events=log_events,
        )

    @queue_app.command("size")
    def queue_size(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
    ) -> None:
        console.print(
            context.queue_factory(
                queue, context.resolve_store_path(store_path, config)
            ).qsize()
        )


def _register_queue_maintenance_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    _register_queue_stats_commands(queue_app, context=context)
    _register_queue_dead_commands(queue_app, context=context)
    _register_queue_requeue_commands(queue_app, context=context)
    _register_queue_purge_commands(queue_app, context=context)


def _register_queue_stats_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    config = context.config

    @queue_app.command("stats")
    def queue_stats(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        watch: bool = typer.Option(False, "--watch"),
        interval: float = typer.Option(1.0, "--interval", min=0.001),
        stale_after: float | None = typer.Option(None, "--stale-after", min=0.0),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        persistent_queue = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        )
        with context.shutdown_state() as shutdown:
            context.print_queue_stats(
                persistent_queue,
                console=console,
                watch=watch,
                interval=interval,
                stale_after=stale_after,
                shutdown=shutdown,
            )

    @queue_app.command("health")
    def queue_health(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        stale_after: float = typer.Option(120.0, "--stale-after", min=0.0),
        json_output: bool = typer.Option(False, "--json"),
    ) -> None:
        persistent_queue = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        )
        stats = persistent_queue.stats()
        worker_health = context.worker_health_summary(
            stats.last_seen_by_worker_id, stale_after=stale_after
        )
        context.print_json(
            console,
            {
                "queue": queue,
                "stats": stats.as_dict(),
                "worker_health": worker_health,
                "dead_letters": context.dead_letter_summary(
                    persistent_queue.dead_letters()
                ),
                "retention": context.retention_settings(config),
            },
        )


def _register_queue_dead_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

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
        persistent_queue = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        )
        if prune_older_than is not None or dry_run:
            older_than = context.resolve_dead_letter_ttl(prune_older_than, config)
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
            context.print_json(
                console,
                {
                    "dry_run": dry_run,
                    "older_than": older_than,
                    "would_delete" if dry_run else "deleted": deleted,
                },
            )
            return
        with context.shutdown_state() as shutdown:
            context.print_dead_letters(
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


def _register_queue_requeue_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

    @queue_app.command("requeue-dead")
    def queue_requeue_dead(
        queue: str,
        message_id: str | None = typer.Argument(None),
        store_path: str | None = typer.Option(None, "--store-path"),
        delay: float = typer.Option(0.0, "--delay", min=0.0),
        all_: bool = typer.Option(False, "--all"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        persistent_queue = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        )
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
                context.emit_event(
                    err_console,
                    "queue.requeue",
                    queue=queue,
                    all=True,
                    requeued=requeued,
                    delay=delay,
                )
            context.print_json(console, {"requeued": requeued})
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
            context.emit_event(
                err_console,
                "queue.requeue",
                queue=queue,
                message_id=message_id,
                delay=delay,
            )
        context.print_json(console, {"id": message_id, "state": "requeued"})


def _register_queue_purge_commands(
    queue_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

    @queue_app.command("purge")
    def queue_purge(
        queue: str,
        store_path: str | None = typer.Option(None, "--store-path"),
        log_events: bool = typer.Option(False, "--log-events"),
    ) -> None:
        deleted = context.queue_factory(
            queue, context.resolve_store_path(store_path, config)
        ).purge()
        if log_events:
            context.emit_event(err_console, "queue.purge", queue=queue, deleted=deleted)
        console.print(deleted)


def register_retry_commands(
    retry_app: Any,
    *,
    context: CommandRegistryContext,
) -> None:
    typer = context.typer
    console = context.console
    err_console = context.err_console
    config = context.config

    @retry_app.command("prune")
    def retry_prune(
        older_than: float | None = typer.Option(None, "--older-than", min=0.0),
        retry_store_path: str | None = typer.Option(None, "--retry-store-path"),
        dry_run: bool = typer.Option(False, "--dry-run"),
    ) -> None:
        resolved_older_than = context.resolve_retry_record_ttl(older_than, config)
        if resolved_older_than is None:
            err_console.print(
                "[red]pass --older-than or configure retry_record_ttl_seconds[/red]"
            )
            raise typer.Exit(1)
        store = SQLiteAttemptStore(
            context.resolve_retry_store_path(retry_store_path, config)
        )
        try:
            now = __import__("time").time()
            deleted = (
                store.count_exhausted_older_than(
                    older_than=resolved_older_than, now=now
                )
                if dry_run
                else store.prune_exhausted(older_than=resolved_older_than, now=now)
            )
        finally:
            store.close()
        context.print_json(
            console,
            {
                "dry_run": dry_run,
                "older_than": resolved_older_than,
                "would_delete" if dry_run else "deleted": deleted,
            },
        )
