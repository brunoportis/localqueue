"""Implementação da fila persistente com lease e retry."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time as _time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import persistqueue
from persistqueue.serializers import json as json_serializer

from simpleq.exceptions import Empty

log = logging.getLogger(__name__)

META_TABLE = "simpleq_meta"
LEASES_TABLE = "simpleq_leases"


@dataclass
class Job:
    """Representa um item retirado da fila para processamento."""

    pqid: int
    data: Any
    attempts: int
    lease_expires_at: float


class SimpleQueue:
    """Fila persistente baseada em SQLite com ACK/NACK, lease e retry.

    A fila utiliza ``persistqueue.SQLiteAckQueue`` para persistência e
    adiciona uma camada de *lease* (aluguel): quando um job é pego, ele fica
    reservado por ``lease_seconds``. Se o worker não responder com ``ack``,
    ``nack`` ou ``fail`` dentro desse prazo, o job volta automaticamente para
    a fila (ou para a dead-letter após ``max_retries`` tentativas).
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        lease_seconds: float = 60.0,
        max_retries: int = 3,
        fsync: bool = False,
        serializer: Any = json_serializer,
        multithreading: bool = False,
    ) -> None:
        """Inicializa a fila.

        :param path: diretório onde o banco SQLite será armazenado.
        :param name: nome da fila (su fixo da tabela interna).
        :param lease_seconds: tempo de lease para cada job retirado.
        :param max_retries: número máximo de retries antes de dead-letter.
        :param fsync: quando ``True`` usa ``PRAGMA synchronous=FULL`` para
            maior durabilidade contra desligamento abrupto.
        :param serializer: serializador compatível com persist-queue.
        :param multithreading: habilita conexões separadas para put/get.
        """
        self.path = Path(path)
        self.name = name
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self._serializer = serializer
        self._multithreading = multithreading

        self.path.mkdir(parents=True, exist_ok=True)

        self._queue = persistqueue.SQLiteAckQueue(
            str(self.path),
            name=self.name,
            auto_resume=True,
            multithreading=multithreading,
            serializer=serializer,
        )

        self._db_file = self.path / "data.db"
        self._conn = sqlite3.connect(
            str(self._db_file),
            timeout=10.0,
            check_same_thread=False,
        )
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute(
            "PRAGMA synchronous={};".format("FULL" if fsync else "NORMAL")
        )

        self._init_tables()
        self._cleanup_orphan_leases()
        self._aux_lock = threading.Lock()

    def _init_tables(self) -> None:
        """Cria tabelas auxiliares de metadados e leases."""
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {META_TABLE} (
                pqid INTEGER PRIMARY KEY,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {LEASES_TABLE} (
                pqid INTEGER PRIMARY KEY,
                acquired_at REAL NOT NULL,
                expires_at REAL NOT NULL
            )
            """
        )
        self._conn.commit()

    def _cleanup_orphan_leases(self) -> None:
        """Remove leases cujo item não está mais em processamento.

        Isso acontece normalmente no startup, quando ``auto_resume`` devolve
        todos os itens ``unack`` para ``ready``.
        """
        table_name = f"ack_queue_{self.name}"
        with self._conn:
            self._conn.execute(
                f"""
                DELETE FROM {LEASES_TABLE}
                WHERE pqid NOT IN (
                    SELECT _id FROM {table_name} WHERE status = 2
                )
                """
            )

    def _table_name(self) -> str:
        """Nome da tabela interna criada pelo persist-queue."""
        return f"ack_queue_{self.name}"

    def put(self, data: Any) -> int:
        """Adiciona um item à fila.

        :param data: payload a ser enfileirado (deve ser serializável).
        :return: id interno do item na fila.
        """
        _id = self._queue.put(data)
        now = _time.monotonic()
        with self._aux_lock, self._conn:
            self._conn.execute(
                f"""
                INSERT OR IGNORE INTO {META_TABLE} (pqid, attempts, created_at)
                VALUES (?, 0, ?)
                """,
                (_id, now),
            )
        return _id

    def get(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Optional[Job]:
        """Retira um item da fila com lease.

        :param block: se ``True``, bloqueia até haver um item disponível.
        :param timeout: tempo máximo de espera em segundos. Somente usado
            quando ``block=True``.
        :return: instância de :class:`Job` ou ``None`` se ``block=False`` e a
            fila estiver vazia.
        :raises Empty: se ``block=True``, ``timeout`` for definido e a fila
            continuar vazia.
        """
        self.reclaim_expired_leases()

        try:
            raw = self._queue.get(block=block, timeout=timeout, raw=True)
        except persistqueue.exceptions.Empty:
            raise Empty("fila vazia")

        pqid = raw["pqid"]
        now = _time.monotonic()
        expires_at = now + self.lease_seconds

        with self._aux_lock, self._conn:
            self._conn.execute(
                f"""
                INSERT OR IGNORE INTO {META_TABLE} (pqid, attempts, created_at)
                VALUES (?, 0, ?)
                """,
                (pqid, now),
            )
            self._conn.execute(
                f"""
                INSERT OR REPLACE INTO {LEASES_TABLE}
                (pqid, acquired_at, expires_at)
                VALUES (?, ?, ?)
                """,
                (pqid, now, expires_at),
            )
            attempts = self._attempts(pqid)

        return Job(
            pqid=pqid,
            data=raw["data"],
            attempts=attempts,
            lease_expires_at=expires_at,
        )

    def get_nowait(self) -> Optional[Job]:
        """Variação não bloqueante de :meth:`get`."""
        return self.get(block=False)

    def ack(self, job: Job) -> None:
        """Confirma o processamento bem-sucedido de um job.

        :param job: job retornado por :meth:`get`.
        """
        with self._aux_lock, self._conn:
            self._queue.ack(id=job.pqid)
            self._delete_lease(job.pqid)

    def nack(self, job: Job) -> None:
        """Devolve o job à fila (ex.: erro transitório).

        Incrementa o contador de tentativas.
        """
        with self._aux_lock, self._conn:
            self._queue.nack(id=job.pqid)
            self._delete_lease(job.pqid)
            self._increment_attempts(job.pqid)

    def fail(self, job: Job) -> None:
        """Marca o job como falha definitiva (dead-letter).

        :param job: job retornado por :meth:`get`.
        """
        with self._aux_lock, self._conn:
            self._queue.ack_failed(id=job.pqid)
            self._delete_lease(job.pqid)

    def reclaim_expired_leases(self, now: Optional[float] = None) -> int:
        """Recupera jobs cujo lease expirou.

        Jobs expirados são devolvidos à fila (com ``nack``) ou marcados como
        falha definitiva caso tenham atingido ``max_retries``.

        :param now: timestamp monotônico opcional para testes.
        :return: número de leases recuperadas.
        """
        now = now or _time.monotonic()
        expired = self._conn.execute(
            f"SELECT pqid FROM {LEASES_TABLE} WHERE expires_at < ?",
            (now,),
        ).fetchall()

        recovered = 0
        for (pqid,) in expired:
            with self._aux_lock, self._conn:
                attempts = self._attempts(pqid)
                if attempts >= self.max_retries:
                    log.warning(
                        "Job %s atingiu %d tentativas; movendo para dead-letter.",
                        pqid,
                        attempts,
                    )
                    self._queue.ack_failed(id=pqid)
                else:
                    log.info(
                        "Lease do job %s expirou; devolvendo à fila (tentativa %d).",
                        pqid,
                        attempts + 1,
                    )
                    self._queue.nack(id=pqid)
                    self._increment_attempts(pqid)
                self._delete_lease(pqid)
            recovered += 1
        return recovered

    def _attempts(self, pqid: int) -> int:
        row = self._conn.execute(
            f"SELECT attempts FROM {META_TABLE} WHERE pqid = ?", (pqid,)
        ).fetchone()
        return row[0] if row else 0

    def _increment_attempts(self, pqid: int) -> None:
        self._conn.execute(
            f"""
            INSERT INTO {META_TABLE} (pqid, attempts, created_at)
            VALUES (?, 1, ?)
            ON CONFLICT(pqid) DO UPDATE SET attempts = attempts + 1
            """,
            (pqid, _time.monotonic()),
        )

    def _delete_lease(self, pqid: int) -> None:
        self._conn.execute(
            f"DELETE FROM {LEASES_TABLE} WHERE pqid = ?", (pqid,)
        )

    def size(self) -> int:
        """Número de jobs prontos para processamento."""
        return self._queue.size

    def unack_count(self) -> int:
        """Número de jobs atualmente em processamento (com lease ativo)."""
        return self._queue.unack_count()

    def acked_count(self) -> int:
        """Número de jobs processados com sucesso."""
        return self._queue.acked_count()

    def failed_count(self) -> int:
        """Número de jobs em dead-letter."""
        return self._queue.ack_failed_count()

    def stats(self) -> dict[str, int]:
        """Retorna estatísticas da fila."""
        return {
            "ready": self.size(),
            "processing": self.unack_count(),
            "acked": self.acked_count(),
            "failed": self.failed_count(),
        }

    def close(self) -> None:
        """Fecha conexões com o banco."""
        self._queue.close()
        self._conn.close()

    def __enter__(self) -> SimpleQueue:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
