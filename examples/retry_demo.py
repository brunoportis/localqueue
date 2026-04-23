import argparse
import random
import time
from queue import Empty
from typing import Callable
from typing import Any

from localqueue.retry import (
    AttemptStoreLockedError,
    PersistentRetrying,
    PersistentRetryExhausted,
    key_from_argument,
    persistent_retry,
)
from localqueue import PersistentQueue, QueueStoreLockedError
from tenacity import wait_exponential, wait_none


SendEmailFn = Callable[[str, str], None]
ProcessVideoFn = Callable[[str, str], None]
WorkerJobFn = Callable[[str, dict[str, str]], None]
DEFAULT_EMAIL_FAILURE_RATE = 0.8
DEFAULT_VIDEO_FAILURE_RATE = 0.7
DEFAULT_WORKER_FAILURE_RATE = 0.65


def build_fast_examples(
    db_path: str,
    *,
    email_failure_rate: float = DEFAULT_EMAIL_FAILURE_RATE,
    video_failure_rate: float = DEFAULT_VIDEO_FAILURE_RATE,
) -> tuple[SendEmailFn, ProcessVideoFn]:
    @persistent_retry(
        max_tries=3,
        store_path=db_path,
        wait=wait_none(),
        key_fn=key_from_argument("task_id"),
    )
    def send_email_fast(task_id: str, email_address: str) -> None:
        print(f"[send_email_fast] task={task_id} email={email_address}")
        if random.random() < email_failure_rate:
            raise ConnectionError("mail server down")
        print(f"[send_email_fast] task={task_id} sent")

    @persistent_retry(
        max_tries=5,
        store_path=db_path,
        wait=wait_none(),
        key_fn=key_from_argument("task_id"),
    )
    def process_video_fast(task_id: str, file_path: str) -> None:
        print(f"[process_video_fast] task={task_id} file={file_path}")
        if random.random() < video_failure_rate:
            raise ValueError("transcoder busy")
        print(f"[process_video_fast] task={task_id} processed")

    return send_email_fast, process_video_fast


def build_slow_examples(
    db_path: str,
    *,
    email_failure_rate: float = DEFAULT_EMAIL_FAILURE_RATE,
    video_failure_rate: float = DEFAULT_VIDEO_FAILURE_RATE,
) -> tuple[SendEmailFn, ProcessVideoFn]:
    @persistent_retry(
        max_tries=3,
        store_path=db_path,
        wait=wait_exponential(multiplier=1, min=2, max=10),
        key_fn=key_from_argument("task_id"),
    )
    def send_email_slow(task_id: str, email_address: str) -> None:
        print(f"[send_email_slow] task={task_id} email={email_address}")
        if random.random() < email_failure_rate:
            raise ConnectionError("mail server down")
        print(f"[send_email_slow] task={task_id} sent")

    @persistent_retry(
        max_tries=5,
        store_path=db_path,
        wait=wait_exponential(multiplier=1, min=2, max=10),
        key_fn=key_from_argument("task_id"),
    )
    def process_video_slow(task_id: str, file_path: str) -> None:
        print(f"[process_video_slow] task={task_id} file={file_path}")
        if random.random() < video_failure_rate:
            raise ValueError("transcoder busy")
        print(f"[process_video_slow] task={task_id} processed")

    return send_email_slow, process_video_slow


def build_worker_example(
    db_path: str, *, worker_failure_rate: float = DEFAULT_WORKER_FAILURE_RATE
) -> WorkerJobFn:
    @persistent_retry(
        max_tries=4,
        store_path=db_path,
        wait=wait_none(),
        key_fn=key_from_argument("task_id"),
    )
    def process_worker_job(task_id: str, payload: dict[str, str]) -> None:
        print(
            f"[worker] job={task_id} kind={payload['kind']} target={payload['target']}"
        )
        if random.random() < worker_failure_rate:
            raise RuntimeError("transient worker failure")
        print(f"[worker] job={task_id} completed")

    return process_worker_job


def run_pair(
    prefix: str, send_email_fn: SendEmailFn, process_video_fn: ProcessVideoFn
) -> None:
    print(f"\n=== {prefix} ===")
    for i in range(1, 4):
        email_task_id = f"{prefix}:send-email:{i}"
        video_task_id = f"{prefix}:process-video:{i}"

        try:
            _ = send_email_fn(email_task_id, "user@example.com")
        except PersistentRetryExhausted as exc:
            print(f"[{prefix}] {exc.key} exhausted after {exc.attempts} attempts")
        except Exception as exc:
            print(f"[{prefix}] {email_task_id} failed this run: {exc}")

        try:
            _ = process_video_fn(video_task_id, f"video_{i}.mp4")
        except PersistentRetryExhausted as exc:
            print(f"[{prefix}] {exc.key} exhausted after {exc.attempts} attempts")
        except Exception as exc:
            print(f"[{prefix}] {video_task_id} failed this run: {exc}")


def run_demo(
    db_path: str,
    *,
    email_failure_rate: float = DEFAULT_EMAIL_FAILURE_RATE,
    video_failure_rate: float = DEFAULT_VIDEO_FAILURE_RATE,
) -> None:
    send_email_fast, process_video_fast = build_fast_examples(
        db_path,
        email_failure_rate=email_failure_rate,
        video_failure_rate=video_failure_rate,
    )
    send_email_slow, process_video_slow = build_slow_examples(
        db_path,
        email_failure_rate=email_failure_rate,
        video_failure_rate=video_failure_rate,
    )
    run_pair("fast", send_email_fast, process_video_fast)
    run_pair("slow", send_email_slow, process_video_slow)


def run_worker_demo(
    db_path: str, *, worker_failure_rate: float = DEFAULT_WORKER_FAILURE_RATE
) -> None:
    print("\n=== persistent queue worker ===")
    queue: PersistentQueue[dict[str, Any]] = PersistentQueue(
        "worker-jobs",
        store_path=f"{db_path}_queue",
        lease_timeout=2,
    )
    retryer = PersistentRetrying(
        store_path=f"{db_path}_queue_retries",
        max_tries=4,
        wait=wait_none(),
        key_fn=key_from_argument("task_id"),
    )

    if queue.empty():
        print("[queue] enqueue sample jobs")
        _ = queue.put({"kind": "email", "target": "alpha@example.com"})
        _ = queue.put({"kind": "video", "target": "video_2.mp4"})
        _ = queue.put({"kind": "webhook", "target": "customer-3"}, delay=1)
        _ = queue.put({"kind": "invoice", "target": "invoice-4"}, delay=2)
    else:
        print(f"[queue] resuming {queue.qsize()} ready job(s)")

    def process_worker_job(task_id: str, payload: dict[str, str]) -> None:
        print(
            f"[worker] message={task_id} kind={payload['kind']} "
            + f"target={payload['target']}"
        )
        if random.random() < worker_failure_rate:
            raise RuntimeError("transient worker failure")
        print(f"[worker] message={task_id} completed")

    for cycle in range(1, 8):
        _run_worker_cycle(queue, retryer, cycle, process_worker_job)


def _run_worker_cycle(
    queue: PersistentQueue[dict[str, Any]],
    retryer: PersistentRetrying,
    cycle: int,
    process_worker_job: Any,
) -> None:
    print(f"\n[worker] poll cycle {cycle}")
    processed = 0

    while True:
        try:
            message = queue.get_message(block=False)
        except Empty:
            break

        payload = message.value
        try:
            _ = retryer(process_worker_job, message.id, payload)
        except PersistentRetryExhausted as exc:
            _ = queue.dead_letter(message)
            print(
                f"[queue] message={exc.key} moved to dead letter "
                + f"after {exc.attempts} attempts"
            )
        except Exception as exc:
            _ = queue.release(message, delay=1)
            print(f"[queue] message={message.id} released for retry: {exc}")
        else:
            _ = queue.ack(message)
            print(f"[queue] message={message.id} acked")
        processed += 1

    if processed == 0:
        print("[worker] no tasks available")
    time.sleep(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="localqueue demo CLI; queue code lives in localqueue/ and retry code in localqueue/retry/"
    )
    _ = parser.add_argument(
        "--worker",
        action="store_true",
        help="run a worker-style example instead of the fast/slow demo",
    )
    _ = parser.add_argument(
        "--db-path",
        default="examples/localqueue_retry_demo.sqlite3",
        help="SQLite file for this demo's persisted retry state",
    )
    _ = parser.add_argument(
        "--email-failure-rate",
        type=float,
        default=DEFAULT_EMAIL_FAILURE_RATE,
        help="demo-only probability that email jobs fail",
    )
    _ = parser.add_argument(
        "--video-failure-rate",
        type=float,
        default=DEFAULT_VIDEO_FAILURE_RATE,
        help="demo-only probability that video jobs fail",
    )
    _ = parser.add_argument(
        "--worker-failure-rate",
        type=float,
        default=DEFAULT_WORKER_FAILURE_RATE,
        help="demo-only probability that worker jobs fail",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.worker:
            run_worker_demo(args.db_path, worker_failure_rate=args.worker_failure_rate)
        else:
            run_demo(
                args.db_path,
                email_failure_rate=args.email_failure_rate,
                video_failure_rate=args.video_failure_rate,
            )
    except (AttemptStoreLockedError, QueueStoreLockedError) as exc:
        print(exc)
        print(
            "Tip: rerun with --db-path pointing at a different file, "
            + "for example 'examples/localqueue_retry_demo_2.sqlite3'."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
