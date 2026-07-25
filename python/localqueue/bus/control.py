"""Explicit handler control-flow exceptions."""

from __future__ import annotations

from localqueue.policies import _delay_to_milliseconds


class Retry(Exception):
    """Request another delivery attempt, optionally with a one-off delay."""

    reason: str | None
    after: float | None

    def __init__(
        self, reason: str | None = None, *, after: float | None = None
    ) -> None:
        if reason is not None and not isinstance(reason, str):
            raise TypeError("'reason' must be a string or None")
        if after is not None:
            try:
                _delay_to_milliseconds(after, field="after")
            except TypeError:
                raise TypeError("'after' must be a number or None") from None
            after = float(after)
        super().__init__("" if reason is None else reason)
        self.reason = reason
        self.after = after


class Reject(Exception):
    """Reject the current delivery without an automatic retry."""

    reason: str
    category: str | None

    def __init__(self, reason: str, *, category: str | None = None) -> None:
        if not isinstance(reason, str):
            raise TypeError("'reason' must be a string")
        if not reason.strip():
            raise ValueError("'reason' must be non-empty")
        if category is not None:
            if not isinstance(category, str):
                raise TypeError("'category' must be a string or None")
            if not category.strip():
                raise ValueError("'category' must be non-empty")
        super().__init__(reason)
        self.reason = reason
        self.category = category


__all__ = ["Reject", "Retry"]
