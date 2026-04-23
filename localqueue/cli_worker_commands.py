from __future__ import annotations

import inspect
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from .services.queue_worker import QueueWorkerOptions
from .worker import PersistentWorkerConfig

if TYPE_CHECKING:
    from .queue import PersistentQueue

_EMPTY = inspect.Parameter.empty


@dataclass(frozen=True, slots=True)
class QueueWorkerCliOptions:
    store_path: str | None
    retry_store_path: str | None
    max_jobs: int
    forever: bool
    max_tries: int
    lease_timeout: float
    worker_id: str | None
    block: bool
    timeout: float | None
    idle_sleep: float
    release_delay: float
    min_interval: float
    circuit_breaker_failures: int
    circuit_breaker_cooldown: float
    dead_letter_on_exhaustion: bool
    log_events: bool

    @classmethod
    def from_kwargs(cls, values: dict[str, Any]) -> QueueWorkerCliOptions:
        return cls(
            store_path=values.pop("store_path"),
            retry_store_path=values.pop("retry_store_path"),
            max_jobs=values.pop("max_jobs"),
            forever=values.pop("forever"),
            max_tries=values.pop("max_tries"),
            lease_timeout=values.pop("lease_timeout"),
            worker_id=values.pop("worker_id"),
            block=values.pop("block"),
            timeout=values.pop("timeout"),
            idle_sleep=values.pop("idle_sleep"),
            release_delay=values.pop("release_delay"),
            min_interval=values.pop("min_interval"),
            circuit_breaker_failures=values.pop("circuit_breaker_failures"),
            circuit_breaker_cooldown=values.pop("circuit_breaker_cooldown"),
            dead_letter_on_exhaustion=values.pop("dead_letter_on_exhaustion"),
            log_events=values.pop("log_events"),
        )


def argument(
    name: str,
    annotation: Any,
    *,
    default: Any = _EMPTY,
) -> inspect.Parameter:
    return inspect.Parameter(
        name,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        annotation=annotation,
        default=default,
    )


def queue_worker_option_parameters(typer: Any) -> list[inspect.Parameter]:
    return [
        argument("store_path", str | None, default=typer.Option(None, "--store-path")),
        argument(
            "retry_store_path",
            str | None,
            default=typer.Option(None, "--retry-store-path"),
        ),
        argument("max_jobs", int, default=typer.Option(1, "--max-jobs", min=1)),
        argument("forever", bool, default=typer.Option(False, "--forever")),
        argument("max_tries", int, default=typer.Option(3, "--max-tries", min=1)),
        argument(
            "lease_timeout",
            float,
            default=typer.Option(30.0, "--lease-timeout", min=0.001),
        ),
        argument("worker_id", str | None, default=typer.Option(None, "--worker-id")),
        argument("block", bool, default=typer.Option(False, "--block")),
        argument(
            "timeout", float | None, default=typer.Option(None, "--timeout", min=0.0)
        ),
        argument(
            "idle_sleep",
            float,
            default=typer.Option(1.0, "--idle-sleep", min=0.001),
        ),
        argument(
            "release_delay",
            float,
            default=typer.Option(0.0, "--release-delay", min=0.0),
        ),
        argument(
            "min_interval",
            float,
            default=typer.Option(0.0, "--min-interval", min=0.0),
        ),
        argument(
            "circuit_breaker_failures",
            int,
            default=typer.Option(0, "--circuit-breaker-failures", min=0),
        ),
        argument(
            "circuit_breaker_cooldown",
            float,
            default=typer.Option(0.0, "--circuit-breaker-cooldown", min=0.0),
        ),
        argument(
            "dead_letter_on_exhaustion",
            bool,
            default=typer.Option(
                True,
                "--dead-letter-on-exhaustion/--release-on-exhaustion",
            ),
        ),
        argument("log_events", bool, default=typer.Option(False, "--log-events")),
    ]


def queue_worker_command_signature(
    typer: Any,
    *command_parameters: inspect.Parameter,
) -> Callable[[Callable[..., None]], Callable[..., None]]:
    parameters = [*command_parameters, *queue_worker_option_parameters(typer)]
    signature = inspect.Signature(parameters=parameters, return_annotation=None)
    annotations = {parameter.name: parameter.annotation for parameter in parameters}
    annotations["return"] = None

    def decorator(callback: Callable[..., None]) -> Callable[..., None]:
        @wraps(callback)
        def wrapper(**values: Any) -> None:
            command_values = {
                parameter.name: values.pop(parameter.name)
                for parameter in command_parameters
            }
            cli_options = QueueWorkerCliOptions.from_kwargs(values)
            if values:
                unexpected = ", ".join(sorted(values))
                raise TypeError(f"unexpected CLI option values: {unexpected}")
            callback(**command_values, cli_options=cli_options)

        wrapper.__annotations__ = annotations
        setattr(wrapper, "__signature__", signature)
        return wrapper

    return decorator


def register_queue_process_command(
    queue_app: Any,
    *,
    typer: Any,
    console: Any,
    err_console: Any,
    config: dict[str, Any],
    validate_worker_loop_options: Callable[..., None],
    load_callable: Callable[[str], Callable[[Any], Any]],
    queue_factory: Callable[..., PersistentQueue],
    resolve_store_path: Callable[[str | None, dict[str, Any]], str],
    resolve_retry_store_path: Callable[[str | None, dict[str, Any]], str],
    shutdown_state: Callable[[], Any],
    process_queue_messages: Callable[..., int],
) -> None:
    @queue_app.command("process")
    @queue_worker_command_signature(
        typer,
        argument("queue", str),
        argument("handler", str),
    )
    def queue_process(
        queue: str,
        handler: str,
        cli_options: QueueWorkerCliOptions,
    ) -> None:
        try:
            validate_worker_loop_options(
                max_jobs=cli_options.max_jobs,
                forever=cli_options.forever,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        try:
            worker = load_callable(handler)
        except (AttributeError, ImportError, TypeError, ValueError) as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        persistent_queue = queue_factory(
            queue,
            resolve_store_path(cli_options.store_path, config),
            lease_timeout=cli_options.lease_timeout,
        )
        if cli_options.worker_id is not None:
            persistent_queue.record_worker_heartbeat(cli_options.worker_id)
        worker_policy = PersistentWorkerConfig(
            min_interval=cli_options.min_interval,
            circuit_breaker_failures=cli_options.circuit_breaker_failures,
            circuit_breaker_cooldown=cli_options.circuit_breaker_cooldown,
        )
        with shutdown_state() as shutdown:
            exit_code = process_queue_messages(
                persistent_queue,
                worker,
                console=console,
                err_console=err_console,
                shutdown=shutdown,
                worker_policy=worker_policy,
                options=QueueWorkerOptions(
                    retry_store_path=resolve_retry_store_path(
                        cli_options.retry_store_path, config
                    ),
                    max_jobs=cli_options.max_jobs,
                    forever=cli_options.forever,
                    max_tries=cli_options.max_tries,
                    worker_id=cli_options.worker_id,
                    block=cli_options.block,
                    timeout=cli_options.timeout,
                    idle_sleep=cli_options.idle_sleep,
                    release_delay=cli_options.release_delay,
                    dead_letter_on_exhaustion=cli_options.dead_letter_on_exhaustion,
                    log_events=cli_options.log_events,
                    mode="process",
                ),
            )
        if exit_code:
            raise typer.Exit(exit_code)


def register_queue_exec_command(
    queue_app: Any,
    *,
    typer: Any,
    console: Any,
    err_console: Any,
    config: dict[str, Any],
    validate_worker_loop_options: Callable[..., None],
    command_handler: Callable[[list[str]], Callable[[Any], None]],
    queue_factory: Callable[..., PersistentQueue],
    resolve_store_path: Callable[[str | None, dict[str, Any]], str],
    resolve_retry_store_path: Callable[[str | None, dict[str, Any]], str],
    shutdown_state: Callable[[], Any],
    process_queue_messages: Callable[..., int],
) -> None:
    @queue_app.command("exec")
    @queue_worker_command_signature(
        typer,
        argument("queue", str),
        argument(
            "command",
            list[str],
            default=typer.Argument(
                ...,
                help="Command to run. Use '--' before commands that contain options.",
            ),
        ),
    )
    def queue_exec(
        queue: str,
        command: list[str],
        cli_options: QueueWorkerCliOptions,
    ) -> None:
        try:
            validate_worker_loop_options(
                max_jobs=cli_options.max_jobs,
                forever=cli_options.forever,
            )
        except ValueError as exc:
            err_console.print(f"[red]{exc}[/red]")
            raise typer.Exit(1) from exc
        persistent_queue = queue_factory(
            queue,
            resolve_store_path(cli_options.store_path, config),
            lease_timeout=cli_options.lease_timeout,
        )
        if cli_options.worker_id is not None:
            persistent_queue.record_worker_heartbeat(cli_options.worker_id)
        worker_policy = PersistentWorkerConfig(
            min_interval=cli_options.min_interval,
            circuit_breaker_failures=cli_options.circuit_breaker_failures,
            circuit_breaker_cooldown=cli_options.circuit_breaker_cooldown,
        )
        with shutdown_state() as shutdown:
            exit_code = process_queue_messages(
                persistent_queue,
                command_handler(command),
                console=console,
                err_console=err_console,
                shutdown=shutdown,
                worker_policy=worker_policy,
                options=QueueWorkerOptions(
                    retry_store_path=resolve_retry_store_path(
                        cli_options.retry_store_path, config
                    ),
                    max_jobs=cli_options.max_jobs,
                    forever=cli_options.forever,
                    max_tries=cli_options.max_tries,
                    worker_id=cli_options.worker_id,
                    block=cli_options.block,
                    timeout=cli_options.timeout,
                    idle_sleep=cli_options.idle_sleep,
                    release_delay=cli_options.release_delay,
                    dead_letter_on_exhaustion=cli_options.dead_letter_on_exhaustion,
                    log_events=cli_options.log_events,
                    mode="exec",
                ),
            )
        if exit_code:
            raise typer.Exit(exit_code)
