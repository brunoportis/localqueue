import multiprocessing
import sqlite3
import threading
import time

from localqueue import DeliveryPolicy, SimpleQueue


def hold_write_lock(database_path, ready):
    connection = sqlite3.connect(database_path, timeout=5.0)
    connection.execute("BEGIN IMMEDIATE")
    ready.set()
    time.sleep(0.75)
    connection.rollback()
    connection.close()


def test_put_releases_gil_while_waiting_for_sqlite_lock(tmp_path):
    path = tmp_path / "gil"
    queue = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
    queue.put({"seed": True})

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    holder = context.Process(
        target=hold_write_lock,
        args=(str(path / "localqueue.db"), ready),
    )
    holder.start()
    assert ready.wait(timeout=5)

    ticks = []
    stop = threading.Event()

    def tick():
        while not stop.is_set():
            ticks.append(time.monotonic_ns())
            time.sleep(0.001)

    ticker = threading.Thread(target=tick)
    ticker.start()
    started = time.monotonic_ns()
    queue.put({"while_locked": True})
    finished = time.monotonic_ns()
    stop.set()
    ticker.join(timeout=2)
    holder.join(timeout=5)
    queue.close()

    assert holder.exitcode == 0
    ticks_while_waiting = [tick for tick in ticks if started <= tick <= finished]
    assert len(ticks_while_waiting) >= 50


def test_put_many_releases_gil_while_waiting_for_sqlite_lock(tmp_path):
    path = tmp_path / "gil-batch"
    queue = SimpleQueue(str(path), delivery=DeliveryPolicy(lease_seconds=5.0))
    queue.put({"seed": True})

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    holder = context.Process(
        target=hold_write_lock,
        args=(str(path / "localqueue.db"), ready),
    )
    holder.start()
    assert ready.wait(timeout=5)

    ticks = []
    stop = threading.Event()

    def tick():
        while not stop.is_set():
            ticks.append(time.monotonic_ns())
            time.sleep(0.001)

    ticker = threading.Thread(target=tick)
    ticker.start()
    started = time.monotonic_ns()
    ids = queue.put_many([{"while_locked": i} for i in range(10)])
    finished = time.monotonic_ns()
    stop.set()
    ticker.join(timeout=2)
    holder.join(timeout=5)
    queue.close()

    assert len(ids) == 10
    assert holder.exitcode == 0
    ticks_while_waiting = [tick for tick in ticks if started <= tick <= finished]
    assert len(ticks_while_waiting) >= 50
