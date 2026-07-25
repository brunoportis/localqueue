"""Positive examples for the public generic typing contract."""

import json
from dataclasses import dataclass
from typing import Awaitable, Callable

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
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    FailedDelivery,
    HandlerContext,
    Reject,
    Retry,
    RuntimeContext,
    event,
)


@event(identity="user_id")
class DurableTypedUserCreated(BaseEvent):
    user_id: str


@event(identity=("tenant_id", "user_id"))
class DurableTypedTenantUserCreated(BaseEvent):
    tenant_id: str
    user_id: str


durable_created: DurableTypedUserCreated = DurableTypedUserCreated(user_id="1")
durable_tenant_created: DurableTypedTenantUserCreated = DurableTypedTenantUserCreated(
    tenant_id="acme", user_id="1"
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


class HttpClient:
    async def get(self, path: str) -> None:
        pass


class AppContext(HandlerContext):
    def __init__(self, runtime: RuntimeContext, *, http: HttpClient) -> None:
        super().__init__(runtime)
        self.http = http


class CallableHandler:
    def __call__(self, event: UserCreated) -> UserCreated:
        return event


event_serializer = EventEnvelopeSerializer()
untrusted_envelope: object = event_serializer.loads(
    event_serializer.dumps({"event_type": "UserCreated", "payload": {}})
)
if isinstance(untrusted_envelope, dict):
    narrowed_event_type = untrusted_envelope.get("event_type")

bus: EventBus[HandlerContext] = EventBus(
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


def direct_handler(event: UserCreated) -> UserCreated:
    return event


registered_direct: Callable[[UserCreated], UserCreated] = bus.on(
    UserCreated,
    direct_handler,
    subscription="users_direct",
)
callable_handler = CallableHandler()
registered_callable: Callable[[UserCreated], UserCreated] = bus.subscription(
    "users_callable"
).handler(UserCreated, callable_handler)


@bus.on("UserCreated", subscription="users_string")
def handle_string_pattern(event: BaseEvent) -> None:
    base_event: BaseEvent = event
    print(base_event.event_type)


@bus.subscription("users_sync").handler(UserCreated)
def sync_emission(event: UserCreated) -> UserCreated:
    return UserCreated(user_id=event.user_id)


@bus.subscription("users_async").handler(UserCreated)
async def async_emission(event: UserCreated) -> UserCreated:
    return UserCreated(user_id=event.user_id)


def awaitable_emission(event: UserCreated) -> Awaitable[BaseEvent | None]:
    async def resolve() -> BaseEvent | None:
        return event

    return resolve()


registered_awaitable: Callable[[UserCreated], Awaitable[BaseEvent | None]] = bus.on(
    UserCreated,
    awaitable_emission,
    subscription="users_direct",
)


registered_event: type[UserCreated] = bus.register(UserCreated)
failed_delivery: FailedDelivery = bus.subscription("users_sync").list_failed()[0]
failed_event: BaseEvent | None = failed_delivery.event
failed_event_type: str | None = failed_delivery.event_type
failed_category: str | None = failed_delivery.failure_category
retry = Retry("temporarily unavailable", after=30)
retry_after: float | None = retry.after
reject = Reject("invalid input", category="validation")
reject_category: str | None = reject.category


http = HttpClient()


def create_context(runtime: RuntimeContext) -> AppContext:
    return AppContext(runtime, http=http)


typed_bus = EventBus[AppContext](
    "./typing-context-bus",
    topology=BusTopology({"users": [UserCreated]}),
    context_factory=create_context,
)


@typed_bus.subscription("users").handler(UserCreated)
async def handle_user_with_context(event: UserCreated, ctx: AppContext) -> None:
    await ctx.http.get("/")
    event_id: str = ctx.event_id
    attempt: int = ctx.attempt
    handler_name: str = ctx.handler_name
    print(event_id, attempt, handler_name)


class UserIndexed(BaseEvent):
    user_id: str


ergonomic_bus = EventBus[AppContext](
    "./typing-ergonomic-bus",
    context_factory=create_context,
)


@ergonomic_bus.handler(UserCreated)
def ergonomic_sync(event: UserCreated) -> None:
    print(event.user_id)


@ergonomic_bus.handler(UserCreated, subscription="ergonomic-async")
async def ergonomic_async(event: UserCreated) -> None:
    print(event.user_id)


@ergonomic_bus.handler(UserCreated, subscription="ergonomic-context")
def ergonomic_context(event: UserCreated, ctx: AppContext) -> UserIndexed:
    print(ctx.handler_name)
    return UserIndexed(user_id=event.user_id)


@ergonomic_bus.handler(UserCreated, subscription="ergonomic-awaitable")
def ergonomic_awaitable(
    event: UserCreated,
) -> Awaitable[BaseEvent | None]:
    return awaitable_emission(event)


def ergonomic_direct_handler(event: UserCreated) -> UserIndexed:
    return UserIndexed(user_id=event.user_id)


ergonomic_direct: Callable[[UserCreated], UserIndexed] = ergonomic_bus.handler(
    UserCreated,
    ergonomic_direct_handler,
    subscription="ergonomic-direct",
)
