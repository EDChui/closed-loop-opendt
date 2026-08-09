from abc import ABC
from dataclasses import dataclass
from typing import Generic, TypeVar


class SystemSnapshot(ABC):
    def __eq__(self, value):
        return super().__eq__(value)
    
    def __hash__(self):
        return super().__hash__()


SnapshotType = TypeVar("SnapshotType", bound=SystemSnapshot)


@dataclass(frozen=True)
class ObservedState(Generic[SnapshotType]):
    state_id: str
    revision: int
    observed_at: float
    snapshot: SnapshotType
