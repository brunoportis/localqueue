"""Positive examples for the public generic typing contract."""

import json
from dataclasses import dataclass
from typing import Callable

from localqueue import (
    EnqueueItem,
    FailedMessage,
    FailureReason,
    Job,
    QueueStats,
    Serializer,
    SimpleQueue,
    Worker,
)
from localqueue.bus import BaseEvent, BusTopology, EventBus, FailedDelivery


@dataclass(frozen=True)
class Task:
    name: str


class TaskSerializer:
    def dumps(self, obj: Task, /) -> bytes:
        return obj.name.encode("utf-8")

    def loads(self, data: bytes, /) -> Task:
        return Task(name=data.decode("utf-8"))


task_serializer: Serializer[Task] = TaskSerializer()
queue: SimpleQueue[Task] = SimpleQueue(
    "./typing-queue",
    serializer=task_serializer,
)
queue.put(Task(name="deploy"))
queue.put_many(
    [
        Task(name="test"),
        EnqueueItem[Task](data=Task(name="release"), job_id="release"),
    ]
)

job: Job[Task] = queue.get()
task: Task = job.data
queue.ack(job)
stats: QueueStats = queue.stats()


def process(task_job: Job[Task]) -> None:
    task_data: Task = task_job.data
    print(task_data.name)


worker: Worker[Task] = Worker(queue, process)
worker_queue: SimpleQueue[Task] = worker.queue
failed_messages: list[FailedMessage[Task]] = queue.list_failed()
failed_message = failed_messages[0]
failed_raw: bytes = failed_message.raw_payload
failed_reason: FailureReason = failed_message.reason
if failed_message.decoded:
    failed_task: Task | None = failed_message.data
else:
    failed_error: str | None = failed_message.decode_error


class UserCreated(BaseEvent):
    user_id: str


class EventEnvelopeSerializer:
    def dumps(self, obj: dict[str, object], /) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes, /) -> object:
        return json.loads(data.decode("utf-8"))


class CallableHandler:
    def __call__(self, event: UserCreated) -> int:
        return len(event.user_id)


event_serializer = EventEnvelopeSerializer()
untrusted_envelope: object = event_serializer.loads(
    event_serializer.dumps({"event_type": "UserCreated", "payload": {}})
)
if isinstance(untrusted_envelope, dict):
    narrowed_event_type = untrusted_envelope.get("event_type")

bus = EventBus(
    "./typing-bus",
    topology=BusTopology(
        {
            "users_sync": [UserCreated],
            "users_async": [UserCreated],
            "users_direct": [UserCreated],
            "users_callable": [UserCreated],
            "users_string": [UserCreated],
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


def direct_handler(event: UserCreated) -> str:
    return event.user_id


registered_direct: Callable[[UserCreated], str] = bus.on(
    UserCreated,
    direct_handler,
    subscription="users_direct",
)
callable_handler = CallableHandler()
registered_callable: Callable[[UserCreated], int] = bus.subscription(
    "users_callable"
).handler(UserCreated, callable_handler)


@bus.on("UserCreated", subscription="users_string")
def handle_string_pattern(event: BaseEvent) -> None:
    base_event: BaseEvent = event
    print(base_event.event_type)


registered_event: type[UserCreated] = bus.register(UserCreated)
failed_delivery: FailedDelivery = bus.subscription("users_sync").list_failed()[0]
failed_event: BaseEvent | None = failed_delivery.event
failed_event_type: str | None = failed_delivery.event_type
