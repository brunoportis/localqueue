from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..results import ResultStore


@dataclass(frozen=True, slots=True)
class ResultPolicy:
    stores_result: bool
    returns_cached_result: bool

    def as_dict(self) -> dict[str, object]:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class NoResultPolicy(ResultPolicy):
    """Result policy that does not persist or return handler results."""

    stores_result: bool = False
    returns_cached_result: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "none",
            "stores_result": self.stores_result,
            "returns_cached_result": self.returns_cached_result,
        }


NO_RESULT_POLICY = NoResultPolicy()


@dataclass(frozen=True, slots=True)
class ReturnStoredResult(ResultPolicy):
    """Result policy that stores successful results and returns them on duplicates."""

    stores_result: bool = True
    returns_cached_result: bool = True
    result_store: ResultStore | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "return-stored",
            "stores_result": self.stores_result,
            "returns_cached_result": self.returns_cached_result,
            "result_store": (
                None if self.result_store is None else type(self.result_store).__name__
            ),
        }


RETURN_STORED_RESULT = ReturnStoredResult()
