"""Worker genérico para processar jobs do simpleq."""

from __future__ import annotations

import logging
import time as _time
from typing import Any, Callable, Optional

from simpleq.core import SimpleQueue
from simpleq.exceptions import Empty
from simpleq.job import Job

log = logging.getLogger(__name__)


class Worker:
    """Worker simples que consome jobs de uma :class:`SimpleQueue`.

    O handler recebe o ``Job`` completo. Ele pode levantar exceções para
    sinalizar erro:

    * Qualquer exceção não listada em ``permanent_errors``: o job volta para
      a fila (ou dead-letter se atingir ``max_retries``).
    * Exceção em ``permanent_errors``: o job vai para dead-letter.
    """

    def __init__(
        self,
        queue: SimpleQueue,
        handler: Callable[[Job], Any],
        *,
        permanent_errors: Optional[tuple[type[BaseException], ...]] = None,
        poll_interval: float = 1.0,
        lease_reclaim_interval: float = 10.0,
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        """Inicializa o worker.

        :param queue: fila a ser consumida.
        :param handler: função que processa o job.
        :param permanent_errors: tupla de exceções que devem mover o job para
            dead-letter.
        :param poll_interval: intervalo entre tentativas quando a fila está
            vazia.
        :param lease_reclaim_interval: intervalo entre verificações de leases
            expirados.
        :param heartbeat_interval: se definido, renova o lease
            automaticamente durante o processamento.
        """
        self.queue = queue
        self.handler = handler
        self.permanent_errors = permanent_errors or ()
        self.poll_interval = poll_interval
        self.lease_reclaim_interval = lease_reclaim_interval
        self.heartbeat_interval = heartbeat_interval
        self._stop = False

    def run(self) -> None:
        """Inicia o loop de consumo."""
        last_reclaim = _time.monotonic()
        while not self._stop:
            now = _time.monotonic()
            if now - last_reclaim >= self.lease_reclaim_interval:
                self.queue.reclaim_expired_leases()
                last_reclaim = now

            try:
                job = self.queue.get(block=True, timeout=self.poll_interval)
            except Empty:
                continue

            self._process(job)

    def run_once(self) -> bool:
        """Processa no máximo um job.

        :return: ``True`` se um job foi processado, ``False`` se a fila estava
            vazia.
        """
        self.queue.reclaim_expired_leases()
        try:
            job = self.queue.get(block=False)
        except Empty:
            return False
        self._process(job)
        return True

    def _process(self, job: Job) -> None:
        log.info("Processando job %s (tentativa %d)", job.id, job.attempts)
        try:
            if self.heartbeat_interval is not None:
                result = self._run_with_heartbeat(job)
            else:
                result = self.handler(job)
        except self.permanent_errors:
            log.exception("Erro permanente no job %s", job.id)
            self.queue.fail(job)
        except Exception:
            log.exception("Erro transitório no job %s; devolvendo à fila", job.id)
            self.queue.nack(job)
        else:
            log.debug("Job %s processado com sucesso: %s", job.id, result)
            self.queue.ack(job)

    def _run_with_heartbeat(self, job: Job) -> Any:
        """Executa o handler renovando o lease periodicamente."""
        import threading

        result: list[Any] = []
        error: list[BaseException] = []
        done = threading.Event()

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
                log.warning("Falha ao renovar lease do job %s", job.id)
                break

        if error:
            raise error[0]
        return result[0] if result else None

    def stop(self) -> None:
        """Sinaliza para o worker parar no próximo ciclo."""
        self._stop = True
