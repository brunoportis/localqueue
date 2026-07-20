"""Worker genérico para processar jobs do localqueue."""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from localqueue.core import SimpleQueue
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

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
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        """Inicializa o worker.

        :param queue: fila a ser consumida.
        :param handler: função que processa o job.
        :param permanent_errors: tupla de exceções que devem mover o job para
            dead-letter.
        :param poll_interval: intervalo entre tentativas quando a fila está
            vazia.
        :param heartbeat_interval: se definido, renova o lease
            automaticamente durante o processamento.
        """
        if not poll_interval > 0:
            raise ValueError("'poll_interval' deve ser positivo")
        if heartbeat_interval is not None and not heartbeat_interval > 0:
            raise ValueError("'heartbeat_interval' deve ser positivo")

        self.queue = queue
        self.handler = handler
        self.permanent_errors = permanent_errors or ()
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._stop = False

    def run(self) -> None:
        """Inicia o loop de consumo."""
        while not self._stop:
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
        except LeaseExpired:
            log.warning(
                "Lease do job %s foi perdido; resultado será descartado",
                job.id,
            )
            return
        except self.permanent_errors:
            log.exception("Erro permanente no job %s", job.id)
            self._transition(self.queue.fail, job)
        except Exception:
            log.exception("Erro transitório no job %s; devolvendo à fila", job.id)
            self._transition(self.queue.nack, job)
        else:
            log.debug("Job %s processado com sucesso: %s", job.id, result)
            self._transition(self.queue.ack, job)

    def _transition(
        self, operation: Callable[[Job], None], job: Job
    ) -> None:
        try:
            operation(job)
        except LeaseExpired:
            log.warning(
                "Lease do job %s foi perdido antes da transição; "
                "resultado será descartado",
                job.id,
            )

    def _run_with_heartbeat(self, job: Job) -> Any:
        """Executa o handler renovando o lease periodicamente."""
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
                log.warning("Perdeu o lease do job %s", job.id)
                lease_lost = True
                break

        # Espera o handler terminar, mesmo se o lease foi perdido.
        done.wait()

        if lease_lost:
            raise LeaseExpired(f"job {job.id} perdeu o lease durante o processamento")

        if error:
            raise error[0]
        return result[0] if result else None

    def stop(self) -> None:
        """Sinaliza para o worker parar no próximo ciclo."""
        self._stop = True
