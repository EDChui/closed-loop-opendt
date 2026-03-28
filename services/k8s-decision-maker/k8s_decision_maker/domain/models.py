from abc import ABC
from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, Optional, Any, TypeVar


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


class SimulationResult(ABC):
    ...


ActionType = TypeVar("ActionType", bound=StrEnum)


@dataclass(frozen=True)
class Decision(Generic[ActionType]):
    action: ActionType
    details: Optional[dict] = None


@dataclass(frozen=True)
class DecisionProposal:
    proposal_id: str
    based_on_state_id: str
    candidate_config: Any
    decision: Decision


@dataclass(frozen=True)
class SimulationBatch:
    batch_id: str
    based_on_state_id: str
    proposals: list[DecisionProposal]


@dataclass(frozen=True)
class ProposalOutcome:
    proposal_id: str
    result: SimulationResult


@dataclass(frozen=True)
class SimulationBatchReport:
    batch_id: str
    based_on_state_id: str
    created_at: float
    received_at: float
    outcomes: list[ProposalOutcome]


@dataclass(frozen=True)
class EvaluatedProposal:
    proposal: DecisionProposal
    outcome: ProposalOutcome
