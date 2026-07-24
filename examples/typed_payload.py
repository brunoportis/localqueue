"""Strongly typed queue payload and worker example."""

from dataclasses import dataclass

from localqueue import Job, Serializer, SimpleQueue, Worker

DATA_DIR = "./data/typed-example"


@dataclass(frozen=True)
class Task:
    name: str


class TaskSerializer:
    def dumps(self, obj: Task, /) -> bytes:
        return obj.name.encode("utf-8")

    def loads(self, data: bytes, /) -> Task:
        return Task(name=data.decode("utf-8"))


def process(job: Job[Task]) -> None:
    print(f"Processing {job.data.name}")


def main() -> None:
    serializer: Serializer[Task] = TaskSerializer()
    with SimpleQueue[Task](DATA_DIR, serializer=serializer) as queue:
        queue.put(Task(name="deploy"))
        Worker(queue, process).run_once()


if __name__ == "__main__":
    main()
