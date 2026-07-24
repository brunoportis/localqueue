# Static typing

`localqueue` preserves one payload type through serializers, queues, leased
jobs, and workers:

```text
Serializer[PayloadT]
        ↓
SimpleQueue[PayloadT]
        ↓
Job[PayloadT]
        ↓
Worker[PayloadT]
        ↓
handler(Job[PayloadT])
```

These relationships are checked statically. A type annotation does not inspect
existing database rows, validate serialized bytes, or convert a decoded value
into an arbitrary Python class.

`SimpleQueue[PayloadT].list_failed()` returns
`list[FailedMessage[PayloadT]]`. Since valid payloads may be `None`, use
`record.decoded` instead of `record.data is not None` to detect decode errors.
EventBus subscription inspection returns `FailedDelivery`.

## Strongly typed payloads

Use a serializer that reconstructs the application type:

```python
from dataclasses import dataclass

from localqueue import Job, Serializer, SimpleQueue, Worker


@dataclass(frozen=True)
class Task:
    name: str


class TaskSerializer:
    def dumps(self, obj: Task, /) -> bytes:
        return obj.name.encode("utf-8")

    def loads(self, data: bytes, /) -> Task:
        return Task(name=data.decode("utf-8"))


serializer: Serializer[Task] = TaskSerializer()
queue: SimpleQueue[Task] = SimpleQueue(
    "./data",
    serializer=serializer,
)

queue.put(Task(name="deploy"))
job: Job[Task] = queue.get()
task: Task = job.data


def process(task_job: Job[Task]) -> None:
    print(task_job.data.name)


worker: Worker[Task] = Worker(queue, process)
```

`Serializer` is an invariant protocol because it both consumes and produces
its payload type. `put_many()` accepts a list containing the same payload type
or `EnqueueItem[PayloadT]`; its atomicity, ordering, and deduplication behavior
are unchanged.

## Default JSON serializer

`SimpleQueue("./data")` remains the short dynamic path. `JsonSerializer[T]`
treats `T` as a static trust contract: `json.loads` returns JSON-shaped runtime
values, but it does not validate `T` or reconstruct dataclasses and arbitrary
application classes.

For a known JSON domain, annotate that domain:

```python
from localqueue import SimpleQueue

queue: SimpleQueue[dict[str, object]] = SimpleQueue("./data")
```

Use a custom serializer when `loads()` must reconstruct a stronger application
type. The serializer remains responsible for runtime validation and
reconstruction.

## Typed EventBus handlers

A class pattern carries its concrete subtype into direct and decorator
registration, for synchronous, asynchronous, and callable-object handlers:

```python
from localqueue.bus import BaseEvent, BusTopology, EventBus


class UserCreated(BaseEvent):
    user_id: str


bus = EventBus(
    "./data",
    topology=BusTopology(
        {
            "index": [UserCreated],
            "welcome": [UserCreated],
        }
    ),
)


@bus.on(UserCreated, subscription="index")
def index_user(event: UserCreated) -> None:
    print(event.user_id)


@bus.subscription("welcome").handler(UserCreated)
async def send_welcome(event: UserCreated) -> None:
    print(event.user_id)
```

The same relationship applies to direct registration when used instead of the
equivalent decorators above:

```python
bus.on(UserCreated, index_user, subscription="index")
bus.subscription("welcome").handler(UserCreated, send_welcome)
```

Strings and `"*"` do not carry Python subtype information, so their handlers
receive `BaseEvent`:

```python
string_bus = EventBus(
    "./data",
    topology=BusTopology(
        {
            "named": ["user.created"],
            "audit": ["*"],
        }
    ),
)


@string_bus.on("user.created", subscription="named")
def handle_named_event(event: BaseEvent) -> None: ...


@string_bus.on("*", subscription="audit")
def audit(event: BaseEvent) -> None: ...
```

Pydantic validates a dynamically resolved event class when a persisted
envelope is consumed. Invalid or legacy envelopes still follow the existing
permanent-failure policy. Handler `timeout` remains runtime-validated and is
accepted only for async callables; the public overloads preserve callable
types without attempting to encode that runtime inspection rule.

## Installed wheels

The distribution includes `py.typed` and the native extension stub, so these
contracts are available from an installed wheel without a source
`PYTHONPATH`. To verify an environment:

```bash
python -c 'import localqueue; print(localqueue.__file__)'
pyrefly check consumer.py --progress-bar no
```

The printed path should point into that environment's `site-packages`, not a
source checkout.
