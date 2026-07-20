"""Exceções usadas pelo localqueue."""

from localqueue.localqueue import Empty, LeaseExpired, LocalQueueError

__all__ = ["Empty", "LeaseExpired", "LocalQueueError"]
