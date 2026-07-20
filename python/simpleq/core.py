"""Facade Python para a fila persistente simpleq."""

from __future__ import annotations

import json
import logging
import time as _time
from pathlib import Path
from typing import Any, Optional, Protocol

from simpleq import simpleq as _native
from simpleq.exceptions import Empty, LeaseExpired, SimpleQError
from simpleq.job import Job

log = logging.getLogger(__name__)


class Serializer(Protocol):
    """Protocolo para serializadores compatíveis."""

    def dumps(self, obj: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class JsonSerializer:
    """Serializador JSON padrão."""

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class SimpleQueue:
    """Fila persistente baseada em SQLite com ACK/NACK, lease e retry.

    O motor transacional é implementado em Rust (extensão nativa), garantindo
    atomicidade mesmo com múltiplos processos/threads.
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        lease_seconds: float = 60.0,
        max_retries: int = 3,
        fsync: bool = False,
        serializer: Optional[Serializer] = None,
    ) -> None:
        """Inicializa a fila.

        :param path: diretório onde o banco SQLite será armazenado.
        :param name: nome da fila.
        :param lease_seconds: tempo de lease para cada job retirado.
        :param max_retries: número máximo de tentativas antes de dead-letter.
        :param fsync: quando ``True`` usa ``PRAGMA synchronous=FULL``.
        :param serializer: serializador com métodos ``dumps`` e ``loads``.
        """
        self.path = Path(path)
        self.name = name
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self.serializer = serializer or JsonSerializer()

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "simpleq.db"

        self._native: Optional[_native.NativeQueue] = _native.NativeQueue(
            str(db_path),
            name,
            max_attempts=max_retries + 1,
            fsync=fsync,
        )

    def _check_closed(self) -> None:
        if self._native is None:
            raise SimpleQError("fila fechada")

    def put(self, data: Any, job_id: Optional[str] = None) -> int:
        """Adiciona um item à fila.

        :param data: payload a ser enfileirado.
        :param job_id: identificador único opcional para deduplicação.
        :return: id interno do item na fila.
        """
        self._check_closed()
        payload = self.serializer.dumps(data)
        return self._native.put(payload, job_id)

    def get(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Job:
        """Retira um item da fila com lease.

        :param block: se ``True``, bloqueia até haver um item disponível.
        :param timeout: tempo máximo de espera em segundos.
        :return: instância de :class:`Job`.
        :raises Empty: se ``block=False`` e a fila estiver vazia, ou se
            ``block=True`` e ``timeout`` expirar.
        """
        self._check_closed()
        lease_ms = int(self.lease_seconds * 1000)

        if not block:
            lease = self._native.get(lease_ms)
            if lease is None:
                raise Empty("fila vazia")
            return self._to_job(lease)

        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' deve ser não negativo")

        start = _time.monotonic()
        while True:
            lease = self._native.get(lease_ms)
            if lease is not None:
                return self._to_job(lease)

            if timeout is not None:
                elapsed = _time.monotonic() - start
                if elapsed >= timeout:
                    raise Empty("fila vazia")

            _time.sleep(0.01)

    def get_nowait(self) -> Optional[Job]:
        """Variação não bloqueante de :meth:`get`."""
        return self.get(block=False)

    def ack(self, job: Job) -> None:
        """Confirma o processamento bem-sucedido de um job."""
        self._check_closed()
        self._native.ack(job.id, job.receipt)

    def nack(
        self,
        job: Job,
        *,
        delay: float = 0.0,
        last_error: Optional[str] = None,
    ) -> None:
        """Devolve o job à fila (erro transitório).

        Se o job já atingiu ``max_retries``, ele é movido para dead-letter.
        """
        self._check_closed()
        delay_ms = int(delay * 1000)
        self._native.nack(job.id, job.receipt, delay_ms, last_error)

    def fail(self, job: Job, last_error: Optional[str] = None) -> None:
        """Marca o job como falha definitiva (dead-letter)."""
        self._check_closed()
        self._native.fail(job.id, job.receipt, last_error)

    def extend_lease(self, job: Job, seconds: float) -> None:
        """Estende o lease de um job.

        Levanta :class:`LeaseExpired` se o lease já tiver expirado.
        """
        self._check_closed()
        extend_ms = int(seconds * 1000)
        new_expiration = self._native.extend_lease(job.id, job.receipt, extend_ms)
        job.lease_expires_at = new_expiration / 1000.0

    def reclaim_expired_leases(self) -> int:
        """Recupera jobs cujo lease expirou.

        :return: número de leases recuperados.
        """
        self._check_closed()
        return self._native.reclaim_expired(None)

    def stats(self) -> dict[str, int]:
        """Retorna estatísticas da fila."""
        self._check_closed()
        stats = self._native.stats()
        return {
            "ready": stats.ready,
            "processing": stats.processing,
            "acked": stats.acked,
            "failed": stats.failed,
        }

    def _to_job(self, lease: "_native.Lease") -> Job:
        data = self.serializer.loads(lease.payload)
        return Job(
            id=lease.id,
            data=data,
            attempts=max(0, lease.attempts - 1),
            receipt=lease.receipt,
            lease_expires_at=lease.lease_until / 1000.0,
            queue=self,
        )

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        if self._native is not None:
            self._native.close()
            self._native = None

    def __enter__(self) -> SimpleQueue:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
