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
        if not lease_seconds > 0:
            raise ValueError("'lease_seconds' deve ser positivo")
        if max_retries < 0:
            raise ValueError("'max_retries' deve ser não negativo")

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

    def _get_native(self) -> "_native.NativeQueue":
        native = self._native
        if native is None:
            raise SimpleQError("fila fechada")
        return native

    def put(self, data: Any, job_id: Optional[str] = None) -> int:
        """Adiciona um item à fila.

        :param data: payload a ser enfileirado.
        :param job_id: identificador único opcional para deduplicação.
        :return: id interno do item na fila.
        """
        payload = self.serializer.dumps(data)
        return self._get_native().put(payload, job_id)

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
        lease_ms = int(self.lease_seconds * 1000)

        if not block:
            lease = self._get_native().get(lease_ms)
            if lease is None:
                raise Empty("fila vazia")
            return self._to_job(lease)

        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' deve ser não negativo")

        start = _time.monotonic()
        sleep = 0.01
        max_sleep = 0.25
        while True:
            lease = self._get_native().get(lease_ms)
            if lease is not None:
                return self._to_job(lease)

            if timeout is not None:
                elapsed = _time.monotonic() - start
                if elapsed >= timeout:
                    raise Empty("fila vazia")

            _time.sleep(sleep)
            sleep = min(sleep * 1.5, max_sleep)

    def get_nowait(self) -> Job:
        """Variação não bloqueante de :meth:`get`."""
        return self.get(block=False)

    def ack(self, job: Job) -> None:
        """Confirma o processamento bem-sucedido de um job."""
        self._get_native().ack(job.id, job.receipt)

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
        delay_ms = int(delay * 1000)
        self._get_native().nack(job.id, job.receipt, delay_ms, last_error)

    def fail(self, job: Job, last_error: Optional[str] = None) -> None:
        """Marca o job como falha definitiva (dead-letter)."""
        self._get_native().fail(job.id, job.receipt, last_error)

    def extend_lease(self, job: Job, seconds: float) -> None:
        """Estende o lease de um job.

        Levanta :class:`LeaseExpired` se o lease já tiver expirado.
        """
        extend_ms = int(seconds * 1000)
        new_expiration = self._get_native().extend_lease(
            job.id, job.receipt, extend_ms
        )
        job.lease_expires_at = new_expiration / 1000.0

    def reclaim_expired_leases(self) -> int:
        """Recupera jobs cujo lease expirou.

        :return: número de leases recuperados.
        """
        return self._get_native().reclaim_expired(None)

    def stats(self) -> dict[str, int]:
        """Retorna estatísticas da fila."""
        stats = self._get_native().stats()
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

    def purge(self, older_than: float, *, include_failed: bool = False) -> int:
        """Remove mensagens antigas da fila.

        :param older_than: idade máxima em segundos. Mensagens mais antigas
            serão removidas.
        :param include_failed: quando ``True``, também remove mensagens
            ``failed``. Por padrão apenas ``acked`` são removidas.
        :return: número de mensagens removidas.
        """
        if older_than < 0:
            raise ValueError("'older_than' deve ser não negativo")

        older_than_ms = int(older_than * 1000)
        removed = 0
        removed += self._get_native().purge(older_than_ms, 2)  # acked
        if include_failed:
            removed += self._get_native().purge(older_than_ms, 3)  # failed
        return removed

    def list_failed(
        self, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Lista mensagens na dead-letter.

        :param limit: número máximo de mensagens.
        :param offset: deslocamento para paginação.
        :return: lista de dicionários com informações das mensagens.
        """
        if limit < 0:
            raise ValueError("'limit' deve ser não negativo")
        if offset < 0:
            raise ValueError("'offset' deve ser não negativo")

        failed = self._get_native().list_failed(limit, offset)
        return [
            {
                "id": msg.id,
                "data": self.serializer.loads(msg.payload),
                "attempts": msg.attempts,
                "last_error": msg.last_error,
                "created_at": msg.created_at / 1000.0,
                "updated_at": msg.updated_at / 1000.0,
            }
            for msg in failed
        ]

    def retry_failed(self, message_id: int) -> None:
        """Move uma mensagem da dead-letter de volta para a fila.

        :param message_id: id da mensagem a ser retentada.
        """
        self._get_native().retry_failed(message_id)

    def vacuum(self) -> None:
        """Compacta todo o ``simpleq.db`` compartilhado pelas filas.

        A operação pode disputar o lock do SQLite com workers ativos.
        """
        self._get_native().vacuum()

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        if self._native is not None:
            self._native.close()
            self._native = None

    def __enter__(self) -> SimpleQueue:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
