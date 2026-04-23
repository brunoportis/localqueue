from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from queue import Empty
from typing import TYPE_CHECKING, Any

from tenacity import wait_none

from ..failure import is_permanent_failure
from ..retry import PersistentRetryExhausted, PersistentRetrying, SQLiteAttemptStore
from ..worker import (
    PersistentWorkerConfig,
    WorkerPolicyState,
    _record_failure,
    _record_success,
    _sleep_for_policy,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from ..queue import PersistentQueue
    from ..stores import QueueMessage


@dataclass(frozen=True, slots=True)
class ProcessResult:
    processed: bool
    last_error: dict[str, Any] | None = None
    final_state: str | None = None
    permanent_failure: bool = False


@dataclass(slots=True)
class ShutdownState:
    requested: bool = False


@dataclass(frozen=True, slots=True)
class QueueWorkerOptions:
    retry_store_path: str | None
    max_jobs: int
    forever: bool
    max_tries: int
    worker_id: str | None
    block: bool
    timeout: float | None
    idle_sleep: float
    release_delay: float
    dead_letter_on_exhaustion: bool
    log_events: bool = False
    mode: str = "process"


@dataclass(slots=True)
class QueueIterationResult:
    message: QueueMessage
    process_result: ProcessResult


@dataclass(slots=True)
class QueueIterationContext:
    queue: PersistentQueue
    handler: Callable[[Any], Any]
    console: Any
    err_console: Any
    policy_state: WorkerPolicyState
    resolved_policy: PersistentWorkerConfig
    retry_store_path: str | None
    owned_retry_store: SQLiteAttemptStore | None
    max_tries: int
    worker_id: str | None
    block: bool
    timeout: float | None
    idle_sleep: float
    release_delay: float
    dead_letter_on_exhaustion: bool
    log_events: bool
    mode: str
    forever: bool


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
        self.stdout = truncate_output(stdout.strip())
        self.stderr = truncate_output(stderr.strip())
        stderr_message = truncate_output(stderr.strip())
        detail = f": {stderr_message}" if stderr_message else ""
        super().__init__(
            f"command exited with status {exit_code}: {format_command(command)}"
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
        self.args = (f"command not found: {format_command(command)}",)


def process_message(
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
) -> ProcessResult:
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
            return handle_exhausted_message(
                queue,
                message,
                exc,
                release_delay=release_delay,
                dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                log_events=log_events,
                mode=mode,
                err_console=err_console,
            )
        except Exception as exc:
            return handle_failed_message(
                queue,
                message,
                exc,
                retryer,
                release_delay=release_delay,
                dead_letter_on_exhaustion=dead_letter_on_exhaustion,
                log_events=log_events,
                mode=mode,
                err_console=err_console,
            )

        acked = queue.ack(message)
        if log_events and err_console is not None:
            emit_event(
                err_console,
                f"{mode}.ack",
                queue=message.queue,
                message_id=message.id,
                attempts=message.attempts,
                leased_by=message.leased_by,
            )
        return ProcessResult(processed=acked, final_state="acked")
    finally:
        if owned_retry_store is not None:
            owned_retry_store.close()


def handle_exhausted_message(
    queue: PersistentQueue,
    message: QueueMessage,
    exc: PersistentRetryExhausted,
    *,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    log_events: bool,
    mode: str,
    err_console: Any | None,
) -> ProcessResult:
    last_error = error_payload(exc)
    _ = finish_failed_message(
        queue,
        message,
        release_delay=release_delay,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        error=exc,
    )
    emit_process_event(
        err_console,
        f"{mode}.dead_letter",
        enabled=log_events,
        message=message,
        last_error=last_error,
    )
    return ProcessResult(
        processed=False,
        last_error=last_error,
        final_state="dead-letter",
        permanent_failure=False,
    )


def handle_failed_message(
    queue: PersistentQueue,
    message: QueueMessage,
    exc: Exception,
    retryer: PersistentRetrying,
    *,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    log_events: bool,
    mode: str,
    err_console: Any | None,
) -> ProcessResult:
    last_error = error_payload(exc)
    record = retryer.get_record(message.id)
    exhausted = record is not None and record.exhausted
    permanent_failure = is_permanent_failure(exc)
    final_state = finish_retryable_failure(
        queue,
        message,
        exc,
        release_delay=release_delay,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        exhausted=exhausted,
        permanent_failure=permanent_failure,
    )
    emit_process_event(
        err_console,
        f"{mode}.{final_state.replace('-', '_')}",
        enabled=log_events,
        message=message,
        last_error=last_error,
    )
    return ProcessResult(
        processed=False,
        last_error=last_error,
        final_state=final_state,
        permanent_failure=permanent_failure,
    )


def finish_retryable_failure(
    queue: PersistentQueue,
    message: QueueMessage,
    exc: Exception,
    *,
    release_delay: float,
    dead_letter_on_exhaustion: bool,
    exhausted: bool,
    permanent_failure: bool,
) -> str:
    if permanent_failure or exhausted:
        _ = finish_failed_message(
            queue,
            message,
            release_delay=release_delay,
            dead_letter_on_exhaustion=True
            if permanent_failure
            else dead_letter_on_exhaustion,
            error=exc,
        )
        return "dead-letter"
    _ = queue.release(message, delay=release_delay, error=exc)
    return "release"


def emit_process_event(
    err_console: Any | None,
    event: str,
    *,
    enabled: bool,
    message: QueueMessage,
    last_error: dict[str, Any] | None,
) -> None:
    if not enabled or err_console is None:
        return
    emit_event(
        err_console,
        event,
        queue=message.queue,
        message_id=message.id,
        attempts=message.attempts,
        leased_by=message.leased_by,
        last_error=last_error,
    )


def process_queue_messages(
    queue: PersistentQueue,
    handler: Callable[[Any], Any],
    *,
    console: Any,
    err_console: Any,
    shutdown: ShutdownState,
    worker_policy: PersistentWorkerConfig | None = None,
    options: QueueWorkerOptions,
) -> int:
    processed = 0
    policy_state = WorkerPolicyState()
    resolved_policy = worker_policy or PersistentWorkerConfig()
    owned_retry_store: SQLiteAttemptStore | None = None

    try:
        while options.forever or processed < options.max_jobs:
            if shutdown.requested:
                return stop_processing(console, processed, options.forever)

            iteration, owned_retry_store, empty_queue = process_queue_iteration(
                QueueIterationContext(
                    queue=queue,
                    handler=handler,
                    console=console,
                    err_console=err_console,
                    policy_state=policy_state,
                    resolved_policy=resolved_policy,
                    retry_store_path=options.retry_store_path,
                    owned_retry_store=owned_retry_store,
                    max_tries=options.max_tries,
                    worker_id=options.worker_id,
                    block=options.block,
                    timeout=options.timeout,
                    idle_sleep=options.idle_sleep,
                    release_delay=options.release_delay,
                    dead_letter_on_exhaustion=options.dead_letter_on_exhaustion,
                    log_events=options.log_events,
                    mode=options.mode,
                    forever=options.forever,
                )
            )
            if empty_queue:
                if options.forever and options.block:
                    continue
                return handle_empty_queue(err_console, processed)
            assert iteration is not None
            if handle_processed_queue_result(
                queue,
                iteration.message,
                iteration.process_result,
                console=console,
                worker_id=options.worker_id,
                policy_state=policy_state,
            ):
                processed += 1
                continue
            if handle_failed_queue_result(
                queue,
                iteration.message,
                iteration.process_result,
                console=console,
                worker_id=options.worker_id,
                policy_state=policy_state,
                resolved_policy=resolved_policy,
                forever=options.forever,
            ):
                continue
            return 1

        return 0
    finally:
        if owned_retry_store is not None:
            owned_retry_store.close()


def process_queue_iteration(
    context: QueueIterationContext,
) -> tuple[QueueIterationResult | None, SQLiteAttemptStore | None, bool]:
    if context.worker_id is not None:
        context.queue.record_worker_heartbeat(context.worker_id)
    _sleep_for_policy(context.policy_state, context.resolved_policy)

    try:
        message = context.queue.get_message(
            block=context.block,
            timeout=poll_timeout(context.forever, context.block, context.timeout),
            leased_by=context.worker_id,
        )
    except Empty:
        if context.forever and not context.block:
            time.sleep(context.idle_sleep)
        return None, context.owned_retry_store, True

    if context.log_events and context.err_console is not None:
        emit_event(
            context.err_console,
            f"{context.mode}.lease",
            queue=message.queue,
            message_id=message.id,
            leased_by=message.leased_by,
            attempts=message.attempts,
            leased_until=message.leased_until,
        )

    if context.retry_store_path is not None and context.owned_retry_store is None:
        owned_retry_store = SQLiteAttemptStore(context.retry_store_path)
    else:
        owned_retry_store = context.owned_retry_store

    result = process_message(
        context.queue,
        message,
        context.handler,
        retry_store=owned_retry_store,
        retry_store_path=None
        if owned_retry_store is not None
        else context.retry_store_path,
        max_tries=context.max_tries,
        release_delay=context.release_delay,
        dead_letter_on_exhaustion=context.dead_letter_on_exhaustion,
        log_events=context.log_events,
        mode=context.mode,
        err_console=context.err_console,
    )
    return (
        QueueIterationResult(message=message, process_result=result),
        owned_retry_store,
        False,
    )


def stop_processing(console: Any, processed: int, forever: bool) -> int:
    if forever:
        print_json(console, {"state": "stopped", "processed": processed})
    return 0


def handle_empty_queue(err_console: Any, processed: int) -> int:
    if processed == 0 and err_console is not None:
        err_console.print("[yellow]queue is empty[/yellow]")
    return 1 if processed == 0 else 0


def handle_processed_queue_result(
    queue: PersistentQueue,
    message: QueueMessage,
    result: ProcessResult,
    *,
    console: Any,
    worker_id: str | None,
    policy_state: WorkerPolicyState,
) -> bool:
    if not result.processed:
        return False
    _record_success(policy_state)
    if worker_id is not None:
        queue.record_worker_heartbeat(worker_id)
    print_json(console, {"id": message.id, "state": "acked"})
    return True


def handle_failed_queue_result(
    queue: PersistentQueue,
    message: QueueMessage,
    result: ProcessResult,
    *,
    console: Any,
    worker_id: str | None,
    policy_state: WorkerPolicyState,
    resolved_policy: PersistentWorkerConfig,
    forever: bool,
) -> bool:
    _record_failure(
        policy_state,
        resolved_policy,
        permanent=result.permanent_failure,
    )

    payload = (
        message_payload(current_message)
        if (current_message := queue.inspect(message.id)) is not None
        else {"id": message.id, "queue": message.queue}
    )
    print_json(
        console,
        {
            **payload,
            "state": "failed",
            "last_error": result.last_error,
        },
    )
    if worker_id is not None:
        queue.record_worker_heartbeat(worker_id)
    return forever


def poll_timeout(forever: bool, block: bool, timeout: float | None) -> float | None:
    if forever and block and timeout is None:
        return 0.5
    return timeout


def finish_failed_message(
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


def error_payload(error: BaseException | str | None) -> dict[str, Any] | None:
    if error is None:
        return None
    if isinstance(error, _CommandExecutionError):
        return {
            "type": type(error).__name__,
            "module": type(error).__module__,
            "message": str(error),
            "command": error.command,
            "exit_code": error.exit_code,
            "stdout": truncate_output(error.stdout.strip()),
            "stderr": truncate_output(error.stderr.strip()),
        }
    if isinstance(error, BaseException):
        error_type = type(error)
        return {
            "type": error_type.__name__,
            "module": error_type.__module__,
            "message": str(error),
        }
    return {"type": None, "module": None, "message": str(error)}


def command_handler(command: list[str]) -> Callable[[Any], None]:
    if not command:
        raise ValueError("command cannot be empty")

    def run_command(value: Any) -> None:
        try:
            completed = subprocess.run(
                command,
                input=json_dumps(value) + "\n",
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


def message_payload(message: QueueMessage) -> dict[str, Any]:
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


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def format_command(command: list[str]) -> str:
    return " ".join(command)


def truncate_output(value: str, *, limit: int = 500) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def print_json(console: Any, value: Any) -> None:
    console.print_json(json_dumps(value))


def emit_event(console: Any, event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        **{key: value for key, value in fields.items() if value is not None},
    }
    print_json(console, payload)


CommandExecutionError = _CommandExecutionError
CommandNotFoundError = _CommandNotFoundError
