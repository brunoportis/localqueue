"""Incompatible uses that an installed wheel must reject."""

from dataclasses import dataclass

from localqueue import FailedMessage, Job, Serializer, SimpleQueue, Worker
from localqueue.bus import BaseEvent, BusTopology, EventBus


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
queue.put("not a task")
wrong_job: Job[str] = queue.get()


def wrong_worker_handler(job: Job[str]) -> None:
    pass


Worker(queue, wrong_worker_handler)


class UserCreated(BaseEvent):
    user_id: str


class OtherEvent(BaseEvent):
    value: str


bus = EventBus("./bus", topology=BusTopology({"users": [UserCreated]}))


def wrong_event_handler(event: OtherEvent) -> None:
    pass


bus.on(UserCreated, wrong_event_handler, subscription="users")
wrong_failed: list[FailedMessage[str]] = queue.list_failed()
wrong_raw: str = queue.list_failed()[0].raw_payload
queue.retry_failed("1")
bus.subscription("users").retry_failed("1")
