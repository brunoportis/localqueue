"""Positive typing contract resolved exclusively from an installed wheel."""

from dataclasses import dataclass
from typing import Callable

from localqueue import Job, Serializer, SimpleQueue, Worker
from localqueue import localqueue as native
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
job: Job[Task] = queue.get()
task: Task = job.data


def process(task_job: Job[Task]) -> None:
    task_data: Task = task_job.data
    print(task_data.name)


worker: Worker[Task] = Worker(queue, process)
native_version: str = native.__version__
native_queue_type: type[native.NativeQueue] = native.NativeQueue


class UserCreated(BaseEvent):
    user_id: str


bus = EventBus(
    "./bus",
    topology=BusTopology(
        {
            "users_sync": [UserCreated],
            "users_async": [UserCreated],
            "users_direct": [UserCreated],
        }
    ),
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
