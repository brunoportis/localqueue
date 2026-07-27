"""Positive typing contract resolved exclusively from an installed wheel."""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from uuid import UUID

from localqueue import (
    FailedMessage,
    FailureReason,
    Job,
    Serializer,
    SimpleQueue,
    Worker,
)
from localqueue import localqueue as native
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    ExecutionResult,
    FailedDelivery,
    Reject,
    Retry,
    SequenceSource,
)


@dataclass(frozen=True)
class Task:
    name: str


class TaskSerializer:
    def dumps(self, obj: Task, /) -> bytes:
        return obj.name.encode("utf-8")

    def loads(self, data: bytes, /) -> Task:
        return Task(name=data.decode("utf-8"))


serializer: Serializer[Task] = TaskSerializer()
queue: SimpleQueue[Task] = SimpleQueue("./queue", serializer=serializer)
job: Job[Task] = queue.get()
task: Task = job.data


def process(task_job: Job[Task]) -> None:
    task_data: Task = task_job.data
    print(task_data.name)


worker: Worker[Task] = Worker(queue, process)
failed: list[FailedMessage[Task]] = queue.list_failed()
reason: FailureReason = failed[0].reason
raw_payload: bytes = failed[0].raw_payload
native_version: str = native.__version__
native_queue_type: type[native.NativeQueue] = native.NativeQueue


class UserCreated(BaseEvent):
    user_id: str


class EventEnvelopeSerializer:
    def dumps(self, obj: dict[str, object], /) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes, /) -> object:
        return json.loads(data.decode("utf-8"))


event_serializer = EventEnvelopeSerializer()
untrusted_envelope: object = event_serializer.loads(
    event_serializer.dumps({"event_type": "UserCreated", "payload": {}})
)
if isinstance(untrusted_envelope, dict):
    narrowed_event_type = untrusted_envelope.get("event_type")

bus = EventBus(
    "./bus",
    topology=BusTopology(
        {
            "users_sync": [UserCreated],
            "users_async": [UserCreated],
            "users_direct": [UserCreated],
        }
    ),
    serializer=event_serializer,
)


@bus.on(UserCreated, subscription="users_sync")
def handle_user_created(event: UserCreated) -> None:
    user_id: str = event.user_id
    print(user_id)


@bus.subscription("users_async").handler(UserCreated)
async def handle_user_created_async(event: UserCreated) -> None:
    user_created: UserCreated = event
    print(user_created.user_id)


registered_handler: Callable[[UserCreated], None] = bus.on(
    UserCreated,
    handle_user_created,
    subscription="users_direct",
)
delivery: FailedDelivery = bus.subscription("users_sync").list_failed()[0]
failed_event: BaseEvent | None = delivery.event
failure_category: str | None = delivery.failure_category
retry_after: float | None = Retry(after=30).after
reject_category: str | None = Reject("invalid", category="validation").category


@bus.source(
    SequenceSource([UserCreated(user_id="1")], fingerprint="users-v1"),
    checkpoint="users-v1",
)
def users(event: UserCreated) -> UserCreated:
    return event


async def execute_users() -> None:
    result: ExecutionResult = await bus.execute(users)
    execution_id: UUID = result.execution_id
    completed_at: datetime = result.completed_at
    result.raise_for_failures()
    print(execution_id, completed_at)
