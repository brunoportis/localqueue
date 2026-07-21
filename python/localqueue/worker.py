"""Generic worker for processing localqueue jobs."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from localqueue.core import SimpleQueue
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

log = logging.getLogger(__name__)


class Worker:
    """Simple worker that consumes jobs from a :class:`SimpleQueue`.

    The handler receives the complete ``Job`` and can raise exceptions to
    signal failure:

    * Exceptions not listed in ``permanent_errors`` return the job to the
      queue, or to dead-letter after exhausting ``max_retries``.
    * Exceptions in ``permanent_errors`` move the job directly to dead-letter.
    """

    def __init__(
        self,
        queue: SimpleQueue,
        handler: Callable[[Job], Any],
        *,
        permanent_errors: Optional[tuple[type[BaseException], ...]] = None,
        poll_interval: float = 1.0,
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        """Initialize the worker.

        :param queue: queue to consume.
        :param handler: function that processes each job.
        :param permanent_errors: exceptions that move a job to dead-letter.
        :param poll_interval: interval between polls while the queue is empty.
        :param heartbeat_interval: interval used to renew the lease during
            processing. It must be shorter than ``queue.lease_seconds``;
            one-third of the lease is recommended.
        """
        if not poll_interval > 0:
            raise ValueError("'poll_interval' must be positive")
        if heartbeat_interval is not None:
            if not heartbeat_interval > 0:
                raise ValueError("'heartbeat_interval' must be positive")
            if heartbeat_interval >= queue.lease_seconds:
                raise ValueError(
                    "'heartbeat_interval' must be smaller than the queue lease"
                )

        self.queue = queue
        self.handler = handler
        self.permanent_errors = permanent_errors or ()
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._stop = False

    def run(self) -> None:
        """Start the consumption loop."""
        while not self._stop:
            try:
                job = self.queue.get(block=True, timeout=self.poll_interval)
            except Empty:
                continue

            self._process(job)

    def run_once(self) -> bool:
        """Process at most one job.

        :return: ``True`` if a job was processed, otherwise ``False``.
        """
        try:
            job = self.queue.get(block=False)
        except Empty:
            return False
        self._process(job)
        return True

    def _process(self, job: Job) -> None:
        log.info("Processing job %s (attempt %d)", job.id, job.attempts)
        try:
            if self.heartbeat_interval is not None:
                result = self._run_with_heartbeat(job)
            else:
                result = self.handler(job)
        except LeaseExpired:
            log.warning(
                "Job %s lost its lease; discarding the result",
                job.id,
            )
            return
        except self.permanent_errors:
            log.exception("Permanent failure processing job %s", job.id)
            self._transition(self.queue.fail, job)
        except Exception:
            log.exception("Transient failure processing job %s; requeueing", job.id)
            self._transition(self.queue.nack, job)
        else:
            log.debug("Job %s processed successfully: %s", job.id, result)
            self._transition(self.queue.ack, job)

    def _transition(self, operation: Callable[[Job], None], job: Job) -> None:
        try:
            operation(job)
        except LeaseExpired:
            log.warning(
                "Job %s lost its lease before the transition; discarding the result",
                job.id,
            )

    def _run_with_heartbeat(self, job: Job) -> Any:
        """Run the handler while periodically renewing the lease."""
        import threading

        result: list[Any] = []
        error: list[BaseException] = []
        done = threading.Event()
        lease_lost = False

        def target() -> None:
            try:
                result.append(self.handler(job))
            except BaseException as exc:  # noqa: BLE001
                error.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=target, daemon=True)
        thread.start()

        while not done.wait(self.heartbeat_interval):
            try:
                job.extend_lease(self.queue.lease_seconds)
            except Exception:
                log.warning("Lost the lease for job %s", job.id)
                lease_lost = True
                break

        # Espera o handler terminar, mesmo se o lease foi perdido.
        done.wait()

        if lease_lost:
            raise LeaseExpired(f"job {job.id} lost its lease during processing")

        if error:
            raise error[0]
        return result[0] if result else None

    def stop(self) -> None:
        """Signal the worker to stop on its next loop iteration."""
        self._stop = True
