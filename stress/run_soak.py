"""Executa um stress test multiprocesso reproduzível para o localqueue.

Exemplo curto:

    python stress/run_soak.py --messages 1000 --duration 30

O teste usa somente a API pública da fila. Ele pode ser executado localmente
por horas aumentando ``--duration`` e ``--messages``.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import multiprocessing as mp
import os
import random
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from localqueue import Empty, LeaseExpired, LocalQueueError, SimpleQueue  # noqa: E402

from release_gate.markdown import evidence_markdown  # noqa: E402
from release_gate.subject import installed_subject  # noqa: E402


def produce(
    path: str, queue_name: str, first: int, count: int, lease_seconds: float
) -> None:
    queue = SimpleQueue(
        path, name=queue_name, lease_seconds=lease_seconds, max_retries=3
    )
    try:
        for index in range(first, first + count):
            job_id = f"stress-{index}"
            queue.put({"id": job_id, "producer": os.getpid()}, job_id=job_id)
    finally:
        queue.close()


CHILD_COUNTERS = (
    "claims",
    "acks",
    "nacks",
    "fails",
    "lease_losses_ack",
    "lease_losses_nack",
    "lease_losses_fail",
    "sqlite_busy_get",
    "sqlite_busy_ack",
    "sqlite_busy_nack",
    "sqlite_busy_fail",
    "sqlite_busy_retries_total",
    "sqlite_busy_exhausted",
)


def shared_counters(context: Any) -> dict[str, Any]:
    return {name: context.Value("q", 0) for name in CHILD_COUNTERS}


def increment(counter: Any) -> None:
    with counter.get_lock():
        counter.value += 1


def counter_snapshot(counters: dict[str, Any]) -> dict[str, int]:
    return {name: int(counter.value) for name, counter in counters.items()}


def diagnostic_event(
    events: Any | None, kind: str, consumer_id: int, **details: object
) -> None:
    if events is not None:
        event = {"kind": kind, "consumer_id": consumer_id, **details}
        send = getattr(events, "send", None)
        if callable(send):
            send(event)
        else:
            events.put(event)


def is_transient_sqlite_contention(error: LocalQueueError) -> bool:
    """Return true only for SQLite's retryable busy/locked messages."""
    message = " ".join(str(error).casefold().split())
    return message in {"database is locked", "database is busy"}


@dataclasses.dataclass(frozen=True)
class SQLiteRetryPolicy:
    initial_delay_seconds: float = 0.01
    max_delay_seconds: float = 0.1
    max_attempts: int = 2
    native_call_budget_seconds: float = 5.0


DEFAULT_SQLITE_RETRY_POLICY = SQLiteRetryPolicy()


class SQLiteContentionExhausted(LocalQueueError):
    def __init__(
        self,
        operation: str,
        attempts: int,
        calls: int,
        backoff_seconds: float,
        elapsed_seconds: float,
        message: str,
    ) -> None:
        self.operation = operation
        self.attempts = attempts
        self.calls = calls
        self.backoff_seconds = backoff_seconds
        self.elapsed_seconds = elapsed_seconds
        self.message = message
        super().__init__(
            f"sqlite busy retry exhausted during {operation} after {attempts} attempts"
        )


class SQLiteRetryStopped(Exception):
    pass


def _wait_for_stop(stop: Any, delay_seconds: float) -> bool:
    wait = getattr(stop, "wait", None)
    if callable(wait):
        return bool(wait(delay_seconds))
    if stop.is_set():
        return True
    time.sleep(delay_seconds)
    return stop.is_set()


def retry_transient_sqlite_contention(
    operation: str,
    action: Any,
    *,
    stop: Any,
    deadline: float | None,
    policy: SQLiteRetryPolicy,
    counters: dict[str, Any],
    consumer_id: int,
    events: Any | None,
) -> Any:
    """Retry only known transient SQLite contention with bounded backoff."""
    attempts = 0
    calls = 0
    backoff_seconds = 0.0
    started_at = time.monotonic()
    last_message = "database is locked"
    while True:
        if stop.is_set():
            raise SQLiteRetryStopped
        now = time.monotonic()
        if (
            calls > 0
            and deadline is not None
            and now + policy.native_call_budget_seconds > deadline
        ):
            elapsed_seconds = now - started_at
            increment(counters["sqlite_busy_exhausted"])
            diagnostic_event(
                events,
                "sqlite_busy_exhausted",
                consumer_id,
                operation=operation,
                attempts=attempts,
                calls=calls,
                backoff_seconds=backoff_seconds,
                elapsed_seconds=elapsed_seconds,
                message=last_message,
            )
            raise SQLiteContentionExhausted(
                operation,
                attempts,
                calls,
                backoff_seconds,
                elapsed_seconds,
                last_message,
            )
        try:
            calls += 1
            return action()
        except LocalQueueError as error:
            if not is_transient_sqlite_contention(error):
                raise
            attempts += 1
            last_message = " ".join(str(error).split())
            increment(counters[f"sqlite_busy_{operation}"])
            now = time.monotonic()
            if attempts >= policy.max_attempts or (
                deadline is not None and now >= deadline
            ):
                elapsed_seconds = now - started_at
                increment(counters["sqlite_busy_exhausted"])
                diagnostic_event(
                    events,
                    "sqlite_busy_exhausted",
                    consumer_id,
                    operation=operation,
                    attempts=attempts,
                    calls=calls,
                    backoff_seconds=backoff_seconds,
                    elapsed_seconds=elapsed_seconds,
                    message=last_message,
                )
                raise SQLiteContentionExhausted(
                    operation,
                    attempts,
                    calls,
                    backoff_seconds,
                    elapsed_seconds,
                    last_message,
                ) from error
            delay = min(
                policy.initial_delay_seconds * (2 ** (attempts - 1)),
                policy.max_delay_seconds,
            )
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - now))
            if delay > 0 and _wait_for_stop(stop, delay):
                raise SQLiteRetryStopped
            backoff_seconds += delay
            increment(counters["sqlite_busy_retries_total"])


def consume(
    path: str,
    queue_name: str,
    stop: Any,
    seed: int,
    nack_rate: float,
    fail_rate: float,
    crash_rate: float,
    lease_seconds: float,
    consumer_id: int,
    counters: dict[str, Any],
    events: Any | None = None,
    transition_delay_seconds: float = 0.0,
    deadline: float | None = None,
    retry_policy: SQLiteRetryPolicy = DEFAULT_SQLITE_RETRY_POLICY,
) -> None:
    queue = SimpleQueue(
        path, name=queue_name, lease_seconds=lease_seconds, max_retries=3
    )
    rng = random.Random(seed)
    try:
        while not stop.is_set():
            try:
                job = retry_transient_sqlite_contention(
                    "get",
                    lambda: queue.get(block=False),
                    stop=stop,
                    deadline=deadline,
                    policy=retry_policy,
                    counters=counters,
                    consumer_id=consumer_id,
                    events=events,
                )
            except SQLiteRetryStopped:
                return
            except Empty:
                time.sleep(0.005)
                continue

            # This synchronous shared-memory write must precede os._exit(17).
            increment(counters["claims"])
            diagnostic_event(events, "claim", consumer_id)
            transition_deadline = min(
                deadline if deadline is not None else float("inf"),
                time.monotonic() + lease_seconds,
            )
            choice = rng.random()
            if choice < crash_rate:
                # Deliberately bypass cleanup to simulate a crash after claiming.
                os._exit(17)
            if transition_delay_seconds:
                time.sleep(transition_delay_seconds)
            if choice < crash_rate + nack_rate:
                operation = "nack"
                try:
                    retry_transient_sqlite_contention(
                        operation,
                        lambda: queue.nack(
                            job, delay=0.01, last_error="stress transient error"
                        ),
                        stop=stop,
                        deadline=transition_deadline,
                        policy=retry_policy,
                        counters=counters,
                        consumer_id=consumer_id,
                        events=events,
                    )
                except SQLiteRetryStopped:
                    return
                except LeaseExpired:
                    increment(counters[f"lease_losses_{operation}"])
                    diagnostic_event(
                        events, "lease_lost", consumer_id, operation=operation
                    )
                else:
                    increment(counters[f"{operation}s"])
                    diagnostic_event(events, operation, consumer_id)
            elif choice < crash_rate + nack_rate + fail_rate:
                operation = "fail"
                try:
                    retry_transient_sqlite_contention(
                        operation,
                        lambda: queue.fail(job, last_error="stress permanent error"),
                        stop=stop,
                        deadline=transition_deadline,
                        policy=retry_policy,
                        counters=counters,
                        consumer_id=consumer_id,
                        events=events,
                    )
                except SQLiteRetryStopped:
                    return
                except LeaseExpired:
                    increment(counters[f"lease_losses_{operation}"])
                    diagnostic_event(
                        events, "lease_lost", consumer_id, operation=operation
                    )
                else:
                    increment(counters[f"{operation}s"])
                    diagnostic_event(events, operation, consumer_id)
            else:
                operation = "ack"
                try:
                    retry_transient_sqlite_contention(
                        operation,
                        lambda: queue.ack(job),
                        stop=stop,
                        deadline=transition_deadline,
                        policy=retry_policy,
                        counters=counters,
                        consumer_id=consumer_id,
                        events=events,
                    )
                except SQLiteRetryStopped:
                    return
                except LeaseExpired:
                    increment(counters[f"lease_losses_{operation}"])
                    diagnostic_event(
                        events, "lease_lost", consumer_id, operation=operation
                    )
                else:
                    increment(counters[f"{operation}s"])
                    diagnostic_event(events, operation, consumer_id)
    finally:
        queue.close()


def classify_consumer_exit(exit_code: int | None, *, stopping: bool) -> str:
    if exit_code == 17:
        return "intentional_crash"
    if exit_code == 0:
        return "normal_shutdown" if stopping else "premature_normal_exit"
    return "unexpected_consumer_exit"


def handle_consumer_exit(
    exit_code: int | None,
    consumer_id: int,
    restarts: list[int],
    max_restarts: int,
    *,
    stopping: bool,
) -> tuple[str, bool, str | None]:
    classification = classify_consumer_exit(exit_code, stopping=stopping)
    if classification == "intentional_crash" and not stopping:
        restarts[consumer_id] += 1
        if restarts[consumer_id] > max_restarts:
            return (
                classification,
                False,
                f"consumer {consumer_id} exceeded --max-restarts",
            )
        return classification, True, None
    if classification in {"premature_normal_exit", "unexpected_consumer_exit"}:
        return (
            classification,
            False,
            f"{classification}: consumer {consumer_id} exited with code {exit_code}",
        )
    return classification, False, None


def empty_counters() -> dict[str, int]:
    return {
        "claims": 0,
        "acks": 0,
        "nacks": 0,
        "fails": 0,
        "lease_losses_ack": 0,
        "lease_losses_nack": 0,
        "lease_losses_fail": 0,
        "sqlite_busy_get": 0,
        "sqlite_busy_ack": 0,
        "sqlite_busy_nack": 0,
        "sqlite_busy_fail": 0,
        "sqlite_busy_retries_total": 0,
        "sqlite_busy_exhausted": 0,
        "intentional_crashes": 0,
        "unexpected_consumer_exits": 0,
        "normal_shutdowns": 0,
    }


def report_invariant_errors(
    counters: dict[str, int],
    last_stats: dict[str, Any],
    exits: list[dict[str, object]],
    restarts: list[int],
) -> list[str]:
    completed_claims = sum(
        counters[name]
        for name in (
            "acks",
            "nacks",
            "fails",
            "lease_losses_ack",
            "lease_losses_nack",
            "lease_losses_fail",
            "intentional_crashes",
        )
    )
    errors = []
    if counters["claims"] != completed_claims:
        errors.append(f"claims={counters['claims']} but outcomes={completed_claims}")
    if counters["acks"] != last_stats.get("acked"):
        errors.append(
            f"acks={counters['acks']} but last_stats.acked={last_stats.get('acked')}"
        )
    intentional_exits = sum(
        item["classification"] == "intentional_crash" for item in exits
    )
    if counters["intentional_crashes"] != intentional_exits:
        errors.append(
            "intentional_crashes="
            f"{counters['intentional_crashes']} but intentional exits={intentional_exits}"
        )
    if sum(restarts) != counters["intentional_crashes"]:
        errors.append(
            f"sum(restarts)={sum(restarts)} but intentional_crashes="
            f"{counters['intentional_crashes']}"
        )
    return errors


def database_state(path: Path, queue_name: str) -> dict[str, Any]:
    database = path / "localqueue.db"
    with sqlite3.connect(database) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        rows = connection.execute(
            """
            SELECT status, COUNT(*)
            FROM messages
            WHERE queue = ?
            GROUP BY status
            ORDER BY status
            """,
            (queue_name,),
        ).fetchall()
    counts = {str(status): count for status, count in rows}
    return {"integrity": integrity, "counts": counts, "rows": sum(counts.values())}


def validate_args(args: argparse.Namespace) -> None:
    if args.messages < 1:
        raise ValueError("--messages deve ser positivo")
    if args.duration <= 0:
        raise ValueError("--duration deve ser positivo")
    if args.lease_seconds <= 0:
        raise ValueError("--lease-seconds deve ser positivo")
    if args.transition_delay_seconds < 0:
        raise ValueError("--transition-delay-seconds não pode ser negativo")
    if args.sqlite_retry_initial_delay <= 0 or args.sqlite_retry_max_delay <= 0:
        raise ValueError("os atrasos de retry SQLite devem ser positivos")
    if args.sqlite_retry_initial_delay > args.sqlite_retry_max_delay:
        raise ValueError("o atraso inicial SQLite não pode exceder o máximo")
    if args.sqlite_retry_attempts < 1:
        raise ValueError("--sqlite-retry-attempts deve ser positivo")
    if args.sqlite_native_call_budget <= 0:
        raise ValueError("--sqlite-native-call-budget deve ser positivo")
    if args.consumers < 1 or args.producers < 1:
        raise ValueError("--producers e --consumers devem ser positivos")
    if args.nack_rate + args.fail_rate + args.crash_rate > 1:
        raise ValueError("as taxas de erro não podem somar mais que 1")


def run(args: argparse.Namespace) -> dict[str, Any]:
    validate_args(args)
    context = mp.get_context("spawn")
    queue_name = "stress"
    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if args.path is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="localqueue-stress-")
        path = Path(temporary_directory.name)
    else:
        path = Path(args.path)
        path.mkdir(parents=True, exist_ok=True)

    stop = context.Event()
    diagnostics_recv, diagnostics_send = context.Pipe(duplex=False)
    retry_policy = SQLiteRetryPolicy(
        args.sqlite_retry_initial_delay,
        args.sqlite_retry_max_delay,
        args.sqlite_retry_attempts,
        args.sqlite_native_call_budget,
    )
    child_counters = shared_counters(context)
    producers: list[mp.Process] = []
    consumers: list[mp.Process | None] = []
    counters = empty_counters()
    exits: list[dict[str, object]] = []
    recorded_pids: set[int | None] = set()
    failure_reason: str | None = None
    last_state: dict[str, Any] = {}
    started_at = time.monotonic()
    restarts = [0] * args.consumers
    drained = False
    queue: SimpleQueue | None = None
    diagnostics: list[dict[str, object]] = []

    def collect_diagnostics() -> None:
        while diagnostics_recv.poll():
            diagnostics.append(diagnostics_recv.recv())

    def record_exit(
        role: str, identity: int, process: mp.Process, classification: str
    ) -> None:
        recorded_pids.add(process.pid)
        item: dict[str, object] = {
            "role": role,
            "id": identity,
            "pid": process.pid,
            "exit_code": process.exitcode,
            "classification": classification,
        }
        for diagnostic in reversed(diagnostics):
            if (
                diagnostic.get("consumer_id") == identity
                and diagnostic.get("kind") == "sqlite_busy_exhausted"
            ):
                item["sqlite_contention"] = diagnostic
                break
        exits.append(item)

    def start_consumer(consumer_id: int, restart: int = 0) -> mp.Process:
        process = context.Process(
            target=consume,
            args=(
                str(path),
                queue_name,
                stop,
                args.seed + consumer_id + restart * 100_000,
                args.nack_rate,
                args.fail_rate,
                args.crash_rate,
                args.lease_seconds,
                consumer_id,
                child_counters,
                diagnostics_send,
                args.transition_delay_seconds,
                started_at + args.duration,
                retry_policy,
            ),
            name=f"localqueue-consumer-{consumer_id}-{restart}",
        )
        process.start()
        return process

    try:
        messages_per_producer, remainder = divmod(args.messages, args.producers)
        first = 0
        for producer_id in range(args.producers):
            count = messages_per_producer + (producer_id < remainder)
            process = context.Process(
                target=produce,
                args=(str(path), queue_name, first, count, args.lease_seconds),
                name=f"localqueue-producer-{producer_id}",
            )
            process.start()
            producers.append(process)
            first += count
        consumers = [
            start_consumer(consumer_id) for consumer_id in range(args.consumers)
        ]
        queue = SimpleQueue(
            str(path), name=queue_name, lease_seconds=args.lease_seconds, max_retries=3
        )
        observed_producers: set[int] = set()

        while time.monotonic() - started_at < args.duration:
            for producer_id, process in enumerate(producers):
                if producer_id not in observed_producers and not process.is_alive():
                    process.join()
                    observed_producers.add(producer_id)
                    classification = (
                        "normal_shutdown"
                        if process.exitcode == 0
                        else "unexpected_producer_exit"
                    )
                    record_exit("producer", producer_id, process, classification)
                    if process.exitcode != 0:
                        failure_reason = f"producer {producer_id} exited with code {process.exitcode}"

            for consumer_id, process in enumerate(consumers):
                if process is None or process.is_alive() or stop.is_set():
                    continue
                process.join()
                collect_diagnostics()
                classification, should_restart, error = handle_consumer_exit(
                    process.exitcode,
                    consumer_id,
                    restarts,
                    args.max_restarts,
                    stopping=False,
                )
                record_exit("consumer", consumer_id, process, classification)
                if classification == "intentional_crash":
                    counters["intentional_crashes"] += 1
                    if should_restart:
                        consumers[consumer_id] = start_consumer(
                            consumer_id, restarts[consumer_id]
                        )
                    else:
                        failure_reason = error
                elif classification in {
                    "premature_normal_exit",
                    "unexpected_consumer_exit",
                }:
                    counters["unexpected_consumer_exits"] += 1
                    failure_reason = error
                else:
                    consumers[consumer_id] = None

            if failure_reason is not None:
                break
            last_state = queue.stats()
            terminal = last_state["acked"] + last_state["failed"]
            if (
                all(not process.is_alive() for process in producers)
                and last_state["ready"] == 0
                and last_state["processing"] == 0
                and terminal == args.messages
            ):
                drained = True
                break
            time.sleep(0.05)
    except Exception as error:
        failure_reason = (
            failure_reason or f"supervisor error: {type(error).__name__}: {error}"
        )
    finally:
        stop.set()
        for process in producers + [item for item in consumers if item is not None]:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
            if process.exitcode == 0:
                counters["normal_shutdowns"] += 1
            if process.pid not in recorded_pids:
                role = "producer" if process in producers else "consumer"
                identity = (
                    producers.index(process)
                    if role == "producer"
                    else consumers.index(process)
                )
                classification = (
                    "normal_shutdown"
                    if process.exitcode == 0
                    else (
                        "unexpected_producer_exit"
                        if role == "producer"
                        else classify_consumer_exit(process.exitcode, stopping=True)
                    )
                )
                record_exit(role, identity, process, classification)
                if classification == "unexpected_consumer_exit":
                    counters["unexpected_consumer_exits"] += 1
                    failure_reason = failure_reason or (
                        f"unexpected consumer exit: consumer {identity} "
                        f"exited with code {process.exitcode}"
                    )
                elif classification == "unexpected_producer_exit":
                    failure_reason = failure_reason or (
                        f"producer {identity} exited with code {process.exitcode}"
                    )
        if queue is not None:
            queue.close()

    collect_diagnostics()

    try:
        state = database_state(path, queue_name)
    except (sqlite3.Error, OSError) as error:
        state = {
            "integrity": "unavailable",
            "counts": {},
            "rows": 0,
            "error": str(error),
        }
        failure_reason = failure_reason or f"could not inspect database: {error}"
    if failure_reason is None and not drained:
        failure_reason = "soak did not drain before --duration elapsed"
    counters.update(counter_snapshot(child_counters))
    exhausted = [
        item for item in diagnostics if item.get("kind") == "sqlite_busy_exhausted"
    ]
    if exhausted:
        detail = exhausted[0]
        failure_reason = (
            "sqlite contention exhausted: consumer "
            f"{detail['consumer_id']}, operation {detail['operation']}, "
            f"{detail['attempts']} attempts, {detail['elapsed_seconds']:.2f} seconds"
        )
    invariant_errors = report_invariant_errors(counters, last_state, exits, restarts)
    if failure_reason is None and invariant_errors:
        failure_reason = "report invariant failed: " + "; ".join(invariant_errors)
    success = (
        failure_reason is None
        and state["integrity"] == "ok"
        and state["rows"] == args.messages
    )
    result = {
        "success": success,
        "failure_reason": failure_reason,
        "duration_seconds": round(time.monotonic() - started_at, 3),
        "messages": args.messages,
        "producers": args.producers,
        "consumers": args.consumers,
        "restarts": restarts,
        "counters": counters,
        "exits": exits,
        "sqlite_contention_diagnostics": exhausted,
        "last_stats": last_state,
        "database": state,
        "path": str(path),
    }
    if temporary_directory is not None:
        temporary_directory.cleanup()
    return result


def base_result(
    args: argparse.Namespace, error: Exception | None = None
) -> dict[str, Any]:
    return {
        "success": False,
        "failure_reason": str(error) if error is not None else "unknown failure",
        "duration_seconds": 0.0,
        "messages": args.messages,
        "producers": args.producers,
        "consumers": args.consumers,
        "restarts": [0] * args.consumers,
        "counters": empty_counters(),
        "exits": [],
        "last_stats": {},
        "database": {"integrity": "unavailable", "counts": {}, "rows": 0},
        "path": str(args.path) if args.path is not None else None,
    }


def execute(args: argparse.Namespace) -> dict[str, Any]:
    try:
        result = run(args)
    except Exception as error:
        result = base_result(args, error)
    identity = (args.candidate_sha, args.candidate_ref, args.candidate_version)
    if any(identity):
        result["subject"] = {
            "candidate_sha": args.candidate_sha,
            "candidate_ref": args.candidate_ref,
            "candidate_version": args.candidate_version,
        }
        if all(identity):
            try:
                result["subject"] = installed_subject(
                    args.candidate_sha,
                    args.candidate_ref,
                    args.candidate_version,
                    require_wheel=args.require_wheel,
                )
            except Exception as error:
                result["success"] = False
                result["failure_reason"] = result.get("failure_reason") or str(error)
        else:
            result["failure_reason"] = (
                "candidate SHA, ref and version must be provided together"
            )
            result["success"] = False
    result["schema_version"] = 1
    result["configuration"] = {
        "duration_seconds": args.duration,
        "messages": args.messages,
        "producers": args.producers,
        "consumers": args.consumers,
        "nack_rate": args.nack_rate,
        "fail_rate": args.fail_rate,
        "crash_rate": args.crash_rate,
        "max_restarts": args.max_restarts,
        "lease_seconds": args.lease_seconds,
        "transition_delay_seconds": args.transition_delay_seconds,
        "sqlite_retry_initial_delay": args.sqlite_retry_initial_delay,
        "sqlite_retry_max_delay": args.sqlite_retry_max_delay,
        "sqlite_retry_attempts": args.sqlite_retry_attempts,
        "sqlite_native_call_budget": args.sqlite_native_call_budget,
        "seed": args.seed,
    }
    result["summary"] = {
        name: result["counters"][name]
        for name in (
            "sqlite_busy_get",
            "sqlite_busy_ack",
            "sqlite_busy_nack",
            "sqlite_busy_fail",
            "sqlite_busy_retries_total",
            "sqlite_busy_exhausted",
        )
    }
    result["status"] = "passed" if result["success"] else "failed"
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(
            evidence_markdown("Multiprocess soak", result), encoding="utf-8"
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=10_000)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--consumers", type=int, default=8)
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument("--nack-rate", type=float, default=0.05)
    parser.add_argument("--fail-rate", type=float, default=0.01)
    parser.add_argument("--crash-rate", type=float, default=0.01)
    parser.add_argument("--max-restarts", type=int, default=100)
    parser.add_argument("--lease-seconds", type=float, default=5.0)
    parser.add_argument("--transition-delay-seconds", type=float, default=0.0)
    parser.add_argument("--sqlite-retry-initial-delay", type=float, default=0.01)
    parser.add_argument("--sqlite-retry-max-delay", type=float, default=0.1)
    parser.add_argument("--sqlite-retry-attempts", type=int, default=2)
    parser.add_argument("--sqlite-native-call-budget", type=float, default=5.0)
    parser.add_argument("--path", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--candidate-sha")
    parser.add_argument("--candidate-ref")
    parser.add_argument("--candidate-version")
    parser.add_argument("--require-wheel", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    report = execute(parse_args())
    raise SystemExit(0 if report["success"] else 1)
