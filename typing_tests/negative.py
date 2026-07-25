"""Invalid consumer examples; the negative typing check must reject each call."""

from dataclasses import dataclass

from localqueue import FailedMessage, Job, Serializer, SimpleQueue, Worker
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    HandlerContext,
    Reject,
    Retry,
    RuntimeContext,
)


@dataclass(frozen=True)
class Task:
    name: str


class TaskSerializer:
    def dumps(self, obj: Task, /) -> bytes:
        return obj.name.encode("utf-8")

    def loads(self, data: bytes, /) -> Task:
        return Task(name=data.decode("utf-8"))


task_serializer: Serializer[Task] = TaskSerializer()
task_queue: SimpleQueue[Task] = SimpleQueue(
    "./typing-negative-queue",
    serializer=task_serializer,
)
string_queue: SimpleQueue[str] = SimpleQueue("./typing-negative-strings")

task_queue.put("deploy")
task_queue.ack(string_queue.get())


def wrong_worker_handler(job: Job[str]) -> None:
    pass


Worker(task_queue, wrong_worker_handler)


class UserCreated(BaseEvent):
    user_id: str


class OrderPlaced(BaseEvent):
    order_id: str


bus = EventBus(
    "./typing-negative-bus",
    topology=BusTopology({"users": [UserCreated]}),
)


def wrong_event_handler(event: OrderPlaced) -> None:
    pass


bus.on(UserCreated, wrong_event_handler, subscription="users")
bus.subscription("users").handler(UserCreated, wrong_event_handler)

wrong_failed_messages: list[FailedMessage[str]] = task_queue.list_failed()
failed = task_queue.list_failed()[0]
wrong_raw: str = failed.raw_payload
failed.reason = "free-form"
wrong_subscription_result: list[FailedMessage[object]] = bus.subscription(
    "users"
).list_failed()
task_queue.retry_failed("1")
bus.subscription("users").retry_failed("1")
Retry(after="later")
Reject("")
Reject("invalid", category=1)


def returns_string(event: UserCreated) -> str:
    return event.user_id


def returns_integer(event: UserCreated) -> int:
    return 1


def returns_events(event: UserCreated) -> list[BaseEvent]:
    return [event]


class Response:
    pass


def returns_response(event: UserCreated) -> Response:
    return Response()


def returns_object(event: UserCreated) -> object:
    return event


bus.subscription("users").handler(UserCreated, returns_string)
bus.subscription("users").handler(UserCreated, returns_integer)
bus.subscription("users").handler(UserCreated, returns_events)
bus.subscription("users").handler(UserCreated, returns_response)
bus.subscription("users").handler(UserCreated, returns_object)


class AppContext(HandlerContext):
    pass


def create_context(runtime: RuntimeContext) -> AppContext:
    return AppContext(runtime)


typed_bus = EventBus[AppContext](
    "./typing-negative-context-bus",
    topology=BusTopology({"users": [UserCreated]}),
    context_factory=create_context,
)


@typed_bus.subscription("users").handler(UserCreated)
def invalid_context_access(event: UserCreated, ctx: AppContext) -> None:
    ctx.nonexistent_service
