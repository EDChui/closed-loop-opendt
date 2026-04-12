from __future__ import annotations

from enum import Enum
from typing import TypeAlias, Annotated, Literal

from pydantic import BaseModel, Field, Tag, Discriminator


class MetricDirection(str, Enum):
    MIN = "min"
    MAX = "max"


class BaseObjectiveSpec(BaseModel):
    name: str = Field(..., description="Name of the objective metric")
    direction: MetricDirection = Field(..., description="Whether to minimize or maximize this metric")


class WeightedObjectiveSpec(BaseObjectiveSpec):
    weight: float = Field(..., description="Relative importance weight for this objective", ge=0)


class RankedObjectiveSpec(BaseObjectiveSpec):
    priority: int = Field(..., description="Lexicographic priority. Lower numbers are evaluated first.", ge=1)
    tie_tolerance: float = Field(default=0.0,description="Values within this distance are treated as tied for this metric.",ge=0)


class WeightedDecisionPolicy(BaseModel):
    policy_type: Literal["weighted"] = Field("weighted", description="Type of decision policy (weighted)")
    objectives: dict[str, WeightedObjectiveSpec] = Field(..., description="Mapping of objective names to their weighted specifications")

    def get_objectives(self) -> list[WeightedObjectiveSpec]:
        """Get the list of objectives (those with weight > 0)."""
        return [obj for obj in self.objectives.values() if obj.weight > 0]


class RankedDecisionPolicy(BaseModel):
    policy_type: Literal["ranked"] = Field("ranked", description="Type of decision policy (ranked)")
    objectives: dict[str, RankedObjectiveSpec] = Field(..., description="Mapping of objective names to their lexicographic specifications")

    def get_objectives(self) -> list[RankedObjectiveSpec]:
        """Get the list of objectives sorted by their priority."""
        return sorted(self.objectives.values(), key=lambda obj: obj.priority)


DecisionPolicy = Annotated[
    Annotated[WeightedDecisionPolicy, Tag("weighted")] | Annotated[RankedDecisionPolicy, Tag("ranked")],
    Discriminator("policy_type")
]
