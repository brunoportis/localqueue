"""Worker genérico para processar jobs do simpleq."""

from __future__ import annotations

import logging
import time as _time
from typing import Any, Callable, Optional

from simpleq.core import Job, SimpleQueue
from simpleq.exceptions import Empty

log = logging.getLogger(__name__)


class Worker:
    """Worker simples que consome jobs de uma :class:`SimpleQueue`.

    O handler recebe o ``data`` do job. Ele pode levantar exceções para
    sinalizar erro:

    * ``TemporaryError`` (ou qualquer exceção filha de ``Exception`` não
      listada em ``permanent_errors``): o job volta para a fila.
    * Exceção em ``permanent_errors``: o job vai para dead-letter.
    """

    def __init__(
        self,
        queue: SimpleQueue,
        handler: Callable[[Any], Any],
        *,
        permanent_errors: Optional[tuple[type[BaseException], ...]] = None,
        poll_interval: float = 1.0,
        lease_reclaim_interval: float = 10.0,
    ) -> None:
        """Inicializa o worker.

        :param queue: fila a ser consumida.
        :param handler: função que processa o payload do job.
        :param permanent_errors: tupla de exceções que devem mover o job para
            dead-letter.
        :param poll_interval: intervalo entre tentativas quando a fila está
            vazia.
        :param lease_reclaim_interval: intervalo entre verificações de leases
            expirados.
        """
        self.queue = queue
        self.handler = handler
        self.permanent_errors = permanent_errors or ()
        self.poll_interval = poll_interval
        self.lease_reclaim_interval = lease_reclaim_interval
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
        log.info("Processando job %s (tentativa %d)", job.pqid, job.attempts)
        try:
            result = self.handler(job.data)
        except self.permanent_errors:
            log.exception("Erro permanente no job %s", job.pqid)
            self.queue.fail(job)
        except Exception:
            log.exception("Erro transitório no job %s; devolvendo à fila", job.pqid)
            self.queue.nack(job)
        else:
            log.debug("Job %s processado com sucesso: %s", job.pqid, result)
            self.queue.ack(job)

    def stop(self) -> None:
        """Sinaliza para o worker parar no próximo ciclo."""
        self._stop = True
