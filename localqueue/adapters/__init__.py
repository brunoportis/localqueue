"""Adapters: concrete implementations of queue policies for local execution."""

from .notification import AsyncNotification, InProcessNotification
from .threaded import ThreadedDispatcher, shutdown_shared_executors

__all__ = [
    "ThreadedDispatcher",
    "shutdown_shared_executors",
    "InProcessNotification",
    "AsyncNotification",
]
