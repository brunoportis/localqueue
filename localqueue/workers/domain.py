from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, cast

from ..failure import is_permanent_failure
from ..idempotency import IdempotencyRecord, IdempotencyStore

if TYPE_CHECKING:
    from ..queue import PersistentQueue
    from ..results import ResultStore
    from ..stores import QueueMessage


def _result_key(message: QueueMessage) -> str | None:
    if message.dedupe_key is None:
        return None
    return f"dedupe:{message.queue}:{message.dedupe_key}"


@dataclass(slots=True)
class _WorkerPolicyAccess:
    queue: PersistentQueue

    def idempotency_store(self) -> IdempotencyStore | None:
        return cast(
            "IdempotencyStore | None",
            getattr(self.queue.delivery_policy, "idempotency_store", None),
        )

    def result_policy(self) -> Any:
        return getattr(self.queue.delivery_policy, "result_policy", None)

    def commit_policy(self) -> Any:
        return getattr(self.queue.delivery_policy, "commit_policy", None)

    def result_store(self) -> ResultStore | None:
        result_policy = self.result_policy()
        return cast("ResultStore | None", getattr(result_policy, "result_store", None))

    def outbox_store(self) -> ResultStore | None:
        commit_policy = self.commit_policy()
        return cast("ResultStore | None", getattr(commit_policy, "outbox_store", None))

    def prepare_store(self) -> ResultStore | None:
        commit_policy = self.commit_policy()
        return cast("ResultStore | None", getattr(commit_policy, "prepare_store", None))

    def commit_store(self) -> ResultStore | None:
        commit_policy = self.commit_policy()
        return cast("ResultStore | None", getattr(commit_policy, "commit_store", None))

    def saga_store(self) -> ResultStore | None:
        commit_policy = self.commit_policy()
        return cast("ResultStore | None", getattr(commit_policy, "saga_store", None))


@dataclass(slots=True)
class _IdempotencyCoordinator:
    policies: _WorkerPolicyAccess

    def _cached_result(self, record: IdempotencyRecord) -> Any:
        result_store = self.policies.result_store()
        if result_store is not None and record.result_key is not None:
            return result_store.load(record.result_key)
        return record.metadata.get("result")

    def begin(
        self,
        message: QueueMessage,
        *,
        now_fn: Callable[[], float],
        ack_duplicate: Any,
    ) -> tuple[bool, Any]:
        store = self.policies.idempotency_store()
        if store is None or message.dedupe_key is None:
            return False, None
        record = store.load(message.dedupe_key)
        if record is not None and record.status == "succeeded":
            _ = ack_duplicate(message)
            result_policy = self.policies.result_policy()
            if bool(getattr(result_policy, "returns_cached_result", False)):
                return True, self._cached_result(record)
            return True, None
        first_seen_at = now_fn() if record is None else record.first_seen_at
        store.save(
            message.dedupe_key,
            IdempotencyRecord(
                status="pending",
                first_seen_at=first_seen_at,
                metadata={"queue": message.queue, "message_id": message.id},
            ),
        )
        return False, None

    def complete(
        self,
        message: QueueMessage,
        *,
        status: str,
        now_fn: Callable[[], float],
        result: Any = None,
        error_payload: dict[str, Any] | None = None,
    ) -> None:
        store = self.policies.idempotency_store()
        if store is None or message.dedupe_key is None:
            return
        existing = store.load(message.dedupe_key)
        now = now_fn()
        first_seen_at = now if existing is None else existing.first_seen_at
        metadata: dict[str, Any] = {"queue": message.queue, "message_id": message.id}
        result_policy = self.policies.result_policy()
        result_key = None if existing is None else existing.result_key
        if bool(getattr(result_policy, "stores_result", False)) and status == "succeeded":
            result_store = self.policies.result_store()
            if result_store is None:
                metadata["result"] = result
                result_key = "inline"
            else:
                resolved_result_key = _result_key(message)
                if resolved_result_key is not None:
                    result_store.save(resolved_result_key, result)
                    result_key = resolved_result_key
        if error_payload is not None:
            metadata["last_error"] = error_payload
        store.save(
            message.dedupe_key,
            IdempotencyRecord(
                status=cast("Any", status),
                first_seen_at=first_seen_at,
                completed_at=now,
                result_key=result_key,
                metadata=metadata,
            ),
        )


@dataclass(slots=True)
class _CommitCoordinator:
    policies: _WorkerPolicyAccess

    def result_store(self) -> ResultStore | None:
        return self.policies.result_store()

    def commit_outbox(
        self,
        message: QueueMessage,
        *,
        result: Any,
        result_key: str | None,
    ) -> None:
        commit_policy = self.policies.commit_policy()
        if getattr(commit_policy, "mode", None) != "transactional-outbox":
            return
        outbox_store = self.policies.outbox_store()
        if outbox_store is None:
            return
        outbox_key = _result_key(message)
        if outbox_key is None:
            return
        outbox_store.save(
            f"outbox:{outbox_key}",
            {
                "queue": message.queue,
                "message_id": message.id,
                "dedupe_key": message.dedupe_key,
                "result_key": result_key,
                "result": result,
            },
        )

    def commit_two_phase(
        self,
        message: QueueMessage,
        *,
        result: Any,
        result_key: str | None,
    ) -> None:
        commit_policy = self.policies.commit_policy()
        if getattr(commit_policy, "mode", None) != "two-phase":
            return
        prepare_store = self.policies.prepare_store()
        commit_store = self.policies.commit_store()
        if prepare_store is None and commit_store is None:
            return
        phase_key = _result_key(message)
        if phase_key is None:
            return
        prepare_payload = {
            "queue": message.queue,
            "message_id": message.id,
            "dedupe_key": message.dedupe_key,
            "result_key": result_key,
            "result": result,
            "phase": "prepare",
        }
        commit_payload = {
            "queue": message.queue,
            "message_id": message.id,
            "dedupe_key": message.dedupe_key,
            "result_key": result_key,
            "result": result,
            "phase": "commit",
        }
        if prepare_store is not None:
            prepare_store.save(f"prepare:{phase_key}", prepare_payload)
        if commit_store is None:
            return
        commit_store.save(f"commit:{phase_key}", commit_payload)

    def commit_saga(
        self,
        message: QueueMessage,
        *,
        phase: str,
        result: Any = None,
        error_payload: dict[str, Any] | None = None,
    ) -> None:
        commit_policy = self.policies.commit_policy()
        if getattr(commit_policy, "mode", None) != "saga":
            return
        saga_store = self.policies.saga_store()
        if saga_store is None:
            return
        phase_key = _result_key(message)
        if phase_key is None:
            return
        payload: dict[str, Any] = {
            "queue": message.queue,
            "message_id": message.id,
            "dedupe_key": message.dedupe_key,
            "phase": phase,
        }
        if result is not None:
            payload["result"] = result
        if error_payload is not None:
            payload["error"] = error_payload
        saga_store.save(f"saga:{phase}:{phase_key}", payload)

    def commit_success(
        self,
        message: QueueMessage,
        *,
        result: Any,
        result_key: str | None,
    ) -> None:
        self.commit_outbox(message, result=result, result_key=result_key)
        self.commit_two_phase(message, result=result, result_key=result_key)
        self.commit_saga(message, phase="forward", result=result)

    def compensate_failure(
        self, message: QueueMessage, *, error_payload: dict[str, Any] | None
    ) -> None:
        self.commit_saga(message, phase="compensate", error_payload=error_payload)


@dataclass(slots=True)
class _WorkerOutcomeCoordinator:
    queue: PersistentQueue
    config: Any

    def ack_success(self, message: QueueMessage) -> bool:
        return self.queue.ack(message)

    def finish_failure(self, message: QueueMessage, exc: BaseException) -> bool:
        permanent = is_permanent_failure(exc)
        if permanent or self.config.dead_letter_on_failure:
            _ = self.queue.dead_letter(message, error=exc)
            return permanent
        _ = self.queue.release(message, delay=self.config.release_delay, error=exc)
        return permanent
